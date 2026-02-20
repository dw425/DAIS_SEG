// ================================================================
// nifi-xml-helpers.js — XML DOM helper functions for NiFi parsing
// Extracted from monolith lines 957-987
// ================================================================

/**
 * Get the trimmed text content of a direct child element.
 * @param {Element} el - Parent element
 * @param {string} tag - Child tag name
 * @returns {string} Text content or empty string
 */
export function getChildText(el, tag) {
  const child = el.querySelector(':scope > ' + tag);
  return child ? child.textContent.trim() : '';
}

/**
 * Extract properties from a NiFi processor/service XML element.
 * Handles multiple NiFi XML formats:
 *   - Template format: config > properties > entry > key + value
 *   - Snippet-level: properties > entry > key + value
 *   - flowController format: property > name + value
 * @param {Element} el - Processor or service element
 * @returns {Object} Key-value property map
 */
export function extractProperties(el) {
  const props = {};
  // NiFi template format: config > properties > entry > key + value
  el.querySelectorAll('config > properties > entry').forEach(entry => {
    const key = entry.querySelector(':scope > key')?.textContent || '';
    const valEl = entry.querySelector(':scope > value');
    if (key && valEl) props[key] = valEl.textContent || '';
  });
  // Also try direct properties > entry (for controllerServices at snippet level)
  if (!Object.keys(props).length) {
    el.querySelectorAll(':scope > properties > entry').forEach(entry => {
      const key = entry.querySelector(':scope > key')?.textContent || '';
      const valEl = entry.querySelector(':scope > value');
      if (key && valEl) props[key] = valEl.textContent || '';
    });
  }
  // Also handle flowController format: direct <property><name>...</name><value>...</value></property>
  if (!Object.keys(props).length) {
    el.querySelectorAll(':scope > property').forEach(prop => {
      const key = prop.querySelector(':scope > name')?.textContent || '';
      const val = prop.querySelector(':scope > value')?.textContent || '';
      if (key) props[key] = val;
    });
  }
  return props;
}
