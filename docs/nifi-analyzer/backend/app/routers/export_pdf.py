"""PDF export router — generates HTML report for browser print-to-PDF."""

import html
import logging
from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from app.models.processor import CamelModel

router = APIRouter()
logger = logging.getLogger(__name__)


class PDFExportRequest(CamelModel):
    """Request body for PDF export."""
    title: str = "ETL Migration Report"
    sections: list[dict] = []
    summary: str = ""
    platform: str = ""
    file_name: str = ""
    processor_count: int = 0
    confidence: float = 0.0


PDF_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>{title}</title>
<style>
  @page {{ margin: 1in; }}
  body {{
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
    color: #1a1a2e;
    line-height: 1.6;
    max-width: 800px;
    margin: 0 auto;
    padding: 40px;
  }}
  h1 {{ color: #FF4B4B; border-bottom: 2px solid #FF4B4B; padding-bottom: 8px; }}
  h2 {{ color: #333; margin-top: 32px; }}
  .meta {{ color: #666; font-size: 14px; margin-bottom: 24px; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin: 24px 0; }}
  .kpi {{ background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px; padding: 16px; text-align: center; }}
  .kpi-value {{ font-size: 28px; font-weight: 700; color: #FF4B4B; }}
  .kpi-label {{ font-size: 12px; color: #666; text-transform: uppercase; letter-spacing: 1px; }}
  .section {{ margin: 24px 0; padding: 16px; background: #fafafa; border-radius: 8px; border-left: 4px solid #FF4B4B; }}
  table {{ width: 100%; border-collapse: collapse; margin: 16px 0; }}
  th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #e9ecef; }}
  th {{ background: #f1f3f5; font-weight: 600; font-size: 13px; text-transform: uppercase; }}
  .footer {{ margin-top: 48px; padding-top: 16px; border-top: 1px solid #e9ecef; font-size: 12px; color: #999; }}
  @media print {{
    body {{ padding: 0; }}
    .no-print {{ display: none; }}
  }}
</style>
</head>
<body>
<div class="no-print" style="margin-bottom:24px;padding:12px;background:#fff3cd;border-radius:8px;text-align:center;">
  Press <b>Ctrl+P</b> (or Cmd+P) to save as PDF
</div>
<h1>{title}</h1>
<div class="meta">
  Platform: {platform} | File: {file_name} | Generated: {generated_at}
</div>
<div class="kpi-grid">
  <div class="kpi"><div class="kpi-value">{processor_count}</div><div class="kpi-label">Processors</div></div>
  <div class="kpi"><div class="kpi-value">{confidence}%</div><div class="kpi-label">Confidence</div></div>
  <div class="kpi"><div class="kpi-value">{section_count}</div><div class="kpi-label">Sections</div></div>
</div>
{summary_html}
{sections_html}
<div class="footer">
  ETL Migration Platform v5.0.0 &mdash; Report generated on {generated_at}
</div>
</body>
</html>"""


@router.post("/export/pdf")
async def export_pdf(req: PDFExportRequest) -> HTMLResponse:
    """Generate an HTML report suitable for browser print-to-PDF."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    summary_html = ""
    if req.summary:
        summary_html = f'<div class="section"><p>{html.escape(req.summary)}</p></div>'

    sections_html_parts = []
    for section in req.sections:
        title = html.escape(section.get("title", "Section"))
        content = html.escape(section.get("content", ""))
        sections_html_parts.append(f"<h2>{title}</h2><div class='section'><p>{content}</p></div>")

    rendered = PDF_TEMPLATE.format(
        title=html.escape(req.title),
        platform=html.escape(req.platform or "Auto-detected"),
        file_name=html.escape(req.file_name or "N/A"),
        generated_at=now,
        processor_count=req.processor_count,
        confidence=round(req.confidence * 100, 1) if req.confidence <= 1 else round(req.confidence, 1),
        section_count=len(req.sections),
        summary_html=summary_html,
        sections_html="\n".join(sections_html_parts),
    )

    logger.info("PDF export generated: %s", req.title)
    return HTMLResponse(content=rendered)
