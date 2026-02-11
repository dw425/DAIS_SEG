# DAIS_SEG Demo — Improvement Plan

## Overview

This document outlines all enhancements needed to make the SEG demo production-resilient across **20+ source systems**, add **smart fallback parsing**, and introduce a **Lumen_Retro-style interactive tier diagram** in the Blueprint step for environment visualization.

---

## 1. Smart Processing — Resilient Parsing Engine

### 1.1 Core Architecture: Parse Cascade

Every input should go through a **cascading parse pipeline** that tries multiple strategies before giving up. The current approach is: detect format → call one parser → succeed or fail. The new approach:

```
Input → BOM/Encoding Cleanup → Format Detection (ranked candidates)
  → Try Parser #1 (highest confidence) → validate result
    → if tables.length > 0 and columns valid → DONE
  → Try Parser #2 (next candidate) → validate result
    → ...
  → Try Parser #3 (fallback: brute-force DDL scan) → validate result
  → Final fallback: regex column extraction → partial result with warnings
```

**Implementation: `smartParse(content, filename)` function**

```javascript
function smartParse(content, filename) {
  content = cleanInput(content);                    // BOM, encoding, whitespace
  const candidates = rankFormats(content, filename); // ordered list of {format, confidence}
  const errors = [];
  for (const {format, confidence} of candidates) {
    try {
      const result = routeParser(format, content, filename);
      if (result && result.tables.length > 0) {
        const validated = validateParseResult(result);
        if (validated.score > 0.5) {
          validated.result.detected_format = format;
          validated.result.confidence = confidence;
          validated.result.parse_attempts = errors.length + 1;
          return validated.result;
        }
      }
    } catch(e) { errors.push({format, error: e.message}); }
  }
  // Final fallback: brute-force extraction
  return bruteForceExtract(content, filename, errors);
}
```

### 1.2 Input Cleanup (`cleanInput`)

Run before any parsing. Fixes the most common input issues:

| Issue | Fix |
|-------|-----|
| BOM (byte order mark) `\uFEFF` | Strip from start of content |
| Windows line endings `\r\n` | Normalize to `\n` |
| NULL bytes `\x00` | Remove |
| Non-breaking spaces `\u00A0` | Replace with regular space |
| Leading/trailing whitespace | Trim |
| HTML entities in XML (`&gt;`, `&lt;`) | Already handled by DOMParser, but decode for non-XML |
| UTF-16 detection | Warn user, attempt decode |
| Smart quotes `\u201C` `\u201D` | Replace with standard quotes |

### 1.3 Format Detection — Ranked Candidates (`rankFormats`)

Instead of returning a single format, return a **ranked list** with confidence scores:

```javascript
function rankFormats(content, filename) {
  const candidates = [];
  // Extension-based (high confidence)
  if (filename) {
    const ext = filename.split('.').pop().toLowerCase();
    const extMap = {sql:'ddl', ddl:'ddl', hql:'ddl', ...};
    if (extMap[ext]) candidates.push({format: extMap[ext], confidence: 0.9});
  }
  // Content-based detection (each adds a candidate)
  if (/<template[\s>][\s\S]{0,1000}<snippet>/i.test(content))
    candidates.push({format: 'nifi_xml', confidence: 0.95});
  if (/StructType\s*\(/i.test(content))
    candidates.push({format: 'pyspark', confidence: 0.85});
  if (/CREATE\s+.*TABLE\b/i.test(content))
    candidates.push({format: 'ddl', confidence: 0.8});
  // ... all other detections with confidence scores
  // Always include DDL as a low-confidence fallback
  candidates.push({format: 'ddl', confidence: 0.1});
  // Dedupe and sort by confidence descending
  return dedupeByFormat(candidates).sort((a,b) => b.confidence - a.confidence);
}
```

### 1.4 Parse Result Validation (`validateParseResult`)

After every parse attempt, validate the result quality:

```javascript
function validateParseResult(result) {
  let score = 0;
  const warnings = [];
  // Has tables?
  if (result.tables.length > 0) score += 0.3;
  // Tables have columns?
  const withCols = result.tables.filter(t => t.columns.length > 0);
  if (withCols.length === result.tables.length) score += 0.3;
  else warnings.push(`${result.tables.length - withCols.length} tables have no columns`);
  // Columns have recognized types?
  const allCols = result.tables.flatMap(t => t.columns);
  const typedCols = allCols.filter(c => c.data_type && c.data_type !== 'varchar');
  if (typedCols.length > allCols.length * 0.3) score += 0.2;
  // Tables have names?
  const namedTables = result.tables.filter(t => t.name && t.name.length > 0);
  if (namedTables.length === result.tables.length) score += 0.1;
  // No duplicate table names?
  const names = result.tables.map(t => t.name);
  if (new Set(names).size === names.length) score += 0.1;
  return {score, warnings, result};
}
```

---

## 2. Per-Parser Resilience Fixes

### 2.1 `parseDDL` — SQL DDL Parser

**Critical Fixes:**

| Issue | Current Behavior | Fix |
|-------|-----------------|-----|
| String literals with parens: `DEFAULT ')'` | Depth counter broken, table lost | Strip string literals before depth counting |
| Comments with parens: `/* ) */` | Depth counter broken | Strip block comments (`/* */`) and line comments (`--`) before parsing |
| `CREATE TEMP TABLE` (without TEMPORARY) | Not matched by regex | Add `TEMP` to the CREATE TABLE regex alternation |
| Unbalanced parens | Silent failure, no table | Add timeout/limit on depth scan, emit warning |
| `ALTER TABLE ... ADD FOREIGN KEY (col1, col2)` | Only captures col1 | Extract all columns from comma-separated list in parens |
| BOM at start | Regex won't match | Already handled by `cleanInput` |

**Pre-processing step for DDL:**
```javascript
function prepareDDL(content) {
  // Remove block comments (careful with nested)
  let clean = content.replace(/\/\*[\s\S]*?\*\//g, ' ');
  // Remove line comments
  clean = clean.replace(/--[^\n]*/g, '');
  // Replace string literals with placeholders (preserve length for position tracking)
  clean = clean.replace(/'(?:[^'\\]|\\.)*'/g, match => "'"+' '.repeat(match.length-2)+"'");
  return clean;
}
```

### 2.2 `parseCSV` — CSV Metadata Parser

**Critical Fixes:**

| Issue | Current Behavior | Fix |
|-------|-----------------|-----|
| Quoted fields with commas: `"col,name"` | Split on inner comma, corrupted | Implement proper CSV field splitting with quote awareness |
| Header partial match: `our_target_table` matches `target_table` | Wrong column detected | Use exact match or word-boundary match instead of `includes()` |
| PK field case: `'True'` vs `'true'` | Not detected as PK | Lowercase all boolean comparisons |
| Windows CRLF in fields | Trailing `\r` in values | Trim each field value after split |

**Proper CSV field splitter:**
```javascript
function splitCSVLine(line) {
  const fields = [];
  let current = '', inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQuotes && line[i+1] === '"') { current += '"'; i++; }
      else inQuotes = !inQuotes;
    } else if (c === ',' && !inQuotes) { fields.push(current.trim()); current = ''; }
    else current += c;
  }
  fields.push(current.trim());
  return fields;
}
```

### 2.3 `parseJSON` — JSON Schema Parser

**Critical Fixes:**

| Issue | Fix |
|-------|-----|
| Invalid JSON crashes (no try/catch) | Wrap in try/catch at parser level, return error result |
| `tables` is object instead of array | Check type, convert `Object.values()` if needed |
| Missing `name` field on table | Generate name from index: `table_1`, `table_2` |
| Column without name | Skip with warning |
| Type not normalized | Pass through `TYPE_NORM` map |

### 2.4 `parseXML` — Generic XML Parser

**Critical Fixes:**

| Issue | Fix |
|-------|-----|
| `<TABLE>` (all caps) not matched | Use `querySelectorAll('*')` then filter by `localName.toLowerCase()` |
| Namespaced tags `<ns:table>` | Use `getElementsByTagNameNS('*', 'table')` or `*|table` selector |
| `pk="1"` or `pk="yes"` not detected | Normalize boolean attributes: `['true','yes','1','y'].includes(v.toLowerCase())` |
| Malformed XML | Wrap DOMParser in try/catch, fall back to regex extraction |

### 2.5 `parseSimpleYAML` — YAML Parser

**Critical Fixes:**

| Issue | Fix |
|-------|-----|
| Tabs treated as 1-space indent | Convert tabs to spaces (4 or 2) before parsing |
| Mixed tabs/spaces | Detect and normalize to consistent indentation |
| Inline comments `key: value # comment` | Strip `#` comments from value (outside quotes) |
| Multiline strings (`\|`, `>`) | Detect block scalar indicators, collect following indented lines |
| Non-ASCII whitespace | Replace Unicode spaces with ASCII space |

### 2.6 `parsePySpark` — PySpark Schema Parser

**Critical Fixes:**

| Issue | Fix |
|-------|-----|
| `StructType (` with space before paren | Make paren matching more flexible: `StructType\s*\(` — already in regex, verify |
| Nested `StructType` inside `StructField` | Track bracket depth, skip nested StructType definitions |
| `StringType` without `()` | Make `\(\)` optional in field regex |
| Bare types without `Type` suffix | Add secondary regex for `StructField("name", "string", True)` format |
| DataFrame schema inference: `df.schema` | Extract from printSchema-style output if found |

### 2.7 `parseScala` — Scala Case Class Parser

**Critical Fixes:**

| Issue | Fix |
|-------|-----|
| Generic class `User[T](...)` | Skip generic bracket before paren: `case\s+class\s+(\w+)(?:\[.*?\])?\s*\(` |
| `Option[T]` inner type not extracted | Extract T from Option wrapper, use T for type mapping |
| Complex generics `Map[String, Int]` | Recognize as `text` type, don't try to parse inner types |
| CamelCase consecutive caps: `HTTPServer` → `h_t_t_p_server` | Use smarter CamelCase splitter |

### 2.8 `parseDBT` — dbt Model Parser

**Critical Fixes:**

| Issue | Fix |
|-------|-----|
| Unbalanced Jinja `{% for` without `%}` | Use greedy fallback: strip all `{%...` and `{{...` patterns |
| Subquery `SELECT ... FROM (SELECT ... FROM t)` | Use last FROM match, or parse CTEs separately |
| Window functions break column extraction | Strip `OVER(...)` clauses before column parsing |
| CTE pattern `WITH cte AS (SELECT ...)` | Detect CTE pattern, parse final SELECT |

### 2.9 `parseAvro` — Avro Schema Parser

**Critical Fixes:**

| Issue | Fix |
|-------|-----|
| Invalid JSON crashes | Wrap in try/catch |
| Union type `["record","null"]` | Handle array type field at record level |
| Nested records | Recursively flatten nested record fields with dot notation: `address.city` |
| Missing namespace | Default to `'default'` (already done) |

### 2.10 `parseProtobuf` — Protobuf Parser

**Critical Fixes:**

| Issue | Fix |
|-------|-----|
| Nested messages `message Outer { message Inner {} }` | Use bracket-depth-aware extraction, parse recursively |
| `map<K,V>` field type | Detect `map<` prefix, extract key/value types, map to `text` |
| Comments `//` and `/* */` | Strip before parsing |
| `oneof` blocks | Extract fields within oneof as nullable |
| `enum` definitions | Extract values as check_constraints |

### 2.11 `parseNiFiXML` — NiFi Flow Parser

**Critical Fixes (already addressed in recent rewrite):**

| Issue | Fix |
|-------|-----|
| ~~Processors at wrong path~~ | ~~Recursive traversal~~ (DONE) |
| Different NiFi versions | Add fallback paths: try `template>snippet`, then `flowController>rootGroup`, then `snippet` alone |
| Variable substitution `${var}` in SQL | Flag variables, show as-is with indicator |
| `:scope >` selector not supported in older browsers | Add fallback using `children` iteration |

### 2.12 `parseInformaticaXML` & `parseTalendXML`

**Critical Fixes:**

| Issue | Fix |
|-------|-----|
| Namespaced tags `<inf:SOURCE>` | Use `*|SOURCE` or iterate all elements checking localName |
| Case-sensitive boolean attrs | Lowercase all boolean comparisons |
| Missing KEYTYPE → no PK detection | Also check for `<PRIMARYKEY>` child elements |
| NaN from parseInt on empty strings | Use `parseInt(val) \|\| 0` pattern |

---

## 3. Brute-Force Fallback Parser & Adaptive Visualization

### 3.1 Brute-Force Fallback

When all typed parsers fail, attempt generic extraction using regex pattern matching for column definitions and XML attribute scanning.

### 3.2 Adaptive Tier Diagram — Visual Environment Representation

The tier diagram **adapts to ANY input type** that gets dropped in. Rather than a fixed layout, the diagram detects the nature of the parsed data and selects the appropriate visualization strategy:

#### Input-Adaptive Strategies

| Input Type | Visualization | Tiering Logic | Connections |
|-----------|---------------|---------------|-------------|
| **SQL / DDL** | Table dependency graph | Topo-sort by FK depth: Tier 1 = independent, Tier 2+ = dependent | FK relationships between tables |
| **NiFi XML** | Processor flow topology | Classified by role: Source → Route/Transform → Process → Sink | NiFi connections (processor-to-processor) |
| **Informatica XML** | Source-to-target mapping | Sources → Transformations → Targets | Field-level mappings |
| **dbt models** | Model DAG | ref() dependency depth | Model references |
| **No dependencies** | **Flat layout** with interconnection indicators | Single tier, evenly distributed | Visual indicators for any discovered relationships |
| **PySpark / Scala / Avro / Protobuf** | Schema structure | Flat (single table = single node) or grouped by namespace | Type references between schemas |

#### Adaptation Logic

```javascript
function buildTierData(blueprint, parsed) {
  const fmt = parsed.input_format;
  // NiFi: processor flow topology
  if (fmt === 'nifi_xml' && parsed._nifi) return buildNiFiTierData(parsed._nifi);
  // Informatica: source → transform → target
  if (fmt === 'informatica_xml') return buildInformaticaTierData(blueprint);
  // Any source with FK relationships: dependency graph
  if (blueprint.relationships.length > 0) return buildSQLTierData(blueprint);
  // No relationships: flat layout
  return buildFlatTierData(blueprint);
}
```

#### NiFi Processor Flow Visualization

For NiFi inputs, the tier diagram shows the **actual flow topology**:
- **Tier 1 (Ingestion)**: GetFile, GetHTTP, ConsumeKafka, QueryDatabaseTable, ListS3, GenerateFlowFile
- **Tier 2 (Route/Transform)**: RouteOnAttribute, UpdateAttribute, JoltTransformJSON, ConvertRecord, SplitRecord
- **Tier 3 (Processing)**: ExecuteSQL, PutDatabaseRecord, LookupRecord
- **Tier 4 (Output/Sink)**: PutFile, PutHDFS, PutS3Object, PutSQL, PutKafka
- **Connections**: Actual NiFi connections resolved to processor names, with relationship labels

#### SQL Table Relationship Visualization

For SQL/DDL inputs with foreign keys:
- Tables sorted by FK dependency depth (topological sort)
- Nodes show: table name, column count, PK indicator
- Connections: FK arrows with column-level labels
- Cross-tier connections for complex multi-hop dependencies

#### Flat Layout (No Dependencies)

For inputs with no discovered relationships:
- All nodes arranged in a single tier or grid
- Visual grouping by schema/namespace if available
- Interconnection indicators (e.g., shared column names, type references)
- Clean, readable even with many nodes

#### Progressive Evolution

The diagram evolves as the pipeline progresses:

| Step | Diagram Changes |
|------|----------------|
| 2. Blueprint | Source tiers + relationships appear |
| 3. Generate | Row count badges added to each node |
| 4. Conform | Bronze → Silver → Gold tiers added below |
| 5. Validate | Validation tier with confidence score, nodes color-coded by health |

---

## 4. Blueprint Visualization — Interactive Tier Diagram

### 4.1 Reference: Lumen_Retro Architecture

The [Lumen_Retro](https://blueprinttechnologies.github.io/Lumen_Retro/) tier diagram uses:
- **React 18** (CDN, no build step)
- **SVG** for connection lines (Bezier curves)
- **DOM nodes** positioned with flexbox in tier bands
- **Interactive**: hover highlights, click selects, detail panel
- **Single HTML file** — perfect for our GitHub Pages demo

### 4.2 SEG Tier Diagram Design

For every parsed input, generate an **environment flow diagram** showing:

```
┌─────────────────────────────────────────────────────┐
│ TIER 1 — SOURCE TABLES (Independent)                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │customers │  │ orders   │  │ products │          │
│  │ 5 cols   │  │ 7 cols   │  │ 4 cols   │          │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘          │
│       │              │              │                │
├───────┼──────────────┼──────────────┼────────────────┤
│ TIER 2 — RELATIONSHIPS (Foreign Keys)               │
│       │              │              │                │
│       └──────┐  ┌────┘       ┌──────┘                │
│              ▼  ▼            ▼                       │
│         ┌────────────┐  ┌──────────┐                │
│         │order_items │  │ reviews  │                │
│         │ FK→orders  │  │FK→prods  │                │
│         │ FK→products│  │FK→cust   │                │
│         └────────────┘  └──────────┘                │
├──────────────────────────────────────────────────────┤
│ TIER 3 — MEDALLION LAYERS                           │
│  Bronze ──────► Silver ──────► Gold                  │
│  (raw data)     (cleaned)      (aggregated)          │
├──────────────────────────────────────────────────────┤
│ TIER 4 — VALIDATION                                 │
│  Schema ─── Fidelity ─── Quality ─── Pipeline       │
│  Parity      Match        Rules       Integrity     │
│                                                      │
│         ┌─────────────────┐                          │
│         │  95% GREEN ✓    │                          │
│         └─────────────────┘                          │
└──────────────────────────────────────────────────────┘
```

### 4.3 NiFi-Specific Visualization

When a NiFi flow is loaded, the tier diagram shows the **actual flow topology**:

```
┌─────────────────────────────────────────────────────┐
│ TIER 1 — INGESTION PROCESSORS                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │GetFile   │  │GetHTTP   │  │ConsumeKafka│         │
│  └────┬─────┘  └────┬─────┘  └────┬──────┘          │
├───────┼──────────────┼──────────────┼────────────────┤
│ TIER 2 — TRANSFORMATION PROCESSORS                  │
│       └──────────────┼──────────────┘                │
│                      ▼                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │UpdateAttr│  │JoltJSON  │  │SplitRecord│          │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘          │
├───────┼──────────────┼──────────────┼────────────────┤
│ TIER 3 — OUTPUT PROCESSORS                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │PutHDFS   │  │PutSQL    │  │PutS3     │          │
│  └──────────┘  └──────────┘  └──────────┘          │
└─────────────────────────────────────────────────────┘
```

### 4.4 Implementation Plan

**Phase 1: Data Model**

Transform parsed blueprint into a tier diagram data structure:

```javascript
function buildTierData(blueprint) {
  const nodes = [], connections = [], tiers = {};

  // Tier 1: Independent tables (no FK dependencies)
  // Tier 2: Dependent tables (have FKs pointing to Tier 1)
  // Tier 3: Medallion layers (Bronze → Silver → Gold)
  // Tier 4: Validation (Schema, Fidelity, Quality, Pipeline → Score)

  // Topological sort tables by FK dependency depth
  const depthMap = computeDepths(blueprint.tables, blueprint.relationships);

  blueprint.tables.forEach(t => {
    const depth = depthMap[t.name] || 0;
    nodes.push({
      id: t.name,
      name: t.name,
      tier: depth + 1,
      type: 'table',
      columns: t.columns.length,
      rows: t.row_count,
      hasPK: t.columns.some(c => c.is_primary_key),
      fkCount: t.foreign_keys.length
    });
  });

  blueprint.relationships.forEach(r => {
    connections.push({
      from: r.from_table.split('.').pop(),
      to: r.to_table.split('.').pop(),
      type: 'foreign_key',
      columns: r.join_columns
    });
  });

  return {nodes, connections, tiers};
}
```

**Phase 2: React Component (inline, CDN-loaded)**

Add to `index.html` in the Blueprint panel:

```html
<!-- CDN dependencies (loaded once, cached) -->
<script src="https://cdnjs.cloudflare.com/ajax/libs/react/18.2.0/umd/react.production.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/react-dom/18.2.0/umd/react-dom.production.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/babel-standalone/7.23.9/babel.min.js"></script>

<div id="tier-diagram-root"></div>

<script type="text/babel">
const TierDiagram = ({nodes, connections, tiers}) => {
  const containerRef = React.useRef(null);
  const nodeRefs = React.useRef({});
  const [lines, setLines] = React.useState([]);
  const [hoveredNode, setHoveredNode] = React.useState(null);
  const [selectedNode, setSelectedNode] = React.useState(null);

  // Group nodes by tier
  const tierGroups = {};
  nodes.forEach(n => {
    if (!tierGroups[n.tier]) tierGroups[n.tier] = [];
    tierGroups[n.tier].push(n);
  });

  // Recalculate SVG lines when nodes render
  React.useEffect(() => { recalcLines(); }, [nodes, connections]);

  function recalcLines() {
    // For each connection, get source/dest DOM positions
    // Build SVG Bezier path: M start C cp1 cp2 end
    // Apply connection type styling (color, width, dash)
  }

  return (
    <div ref={containerRef} style={{position:'relative', overflow:'auto'}}>
      <svg style={{position:'absolute', top:0, left:0, width:'100%', height:'100%',
                   pointerEvents:'none', zIndex:1}}>
        <defs>
          <marker id="arrow" viewBox="0 0 10 8" refX="10" refY="4"
                  markerWidth="8" markerHeight="6" orient="auto">
            <path d="M0,0 L10,4 L0,8 Z" fill="currentColor"/>
          </marker>
        </defs>
        {lines.map((line, i) => (
          <path key={i} d={line.path} stroke={line.color} strokeWidth={line.width}
                fill="none" markerEnd="url(#arrow)" opacity={line.opacity}/>
        ))}
      </svg>

      {Object.entries(tierGroups).sort(([a],[b]) => a-b).map(([tier, tierNodes]) => (
        <TierBand key={tier} tier={tier} config={tiers[tier]}>
          {tierNodes.map(node => (
            <NodeBox key={node.id} node={node}
                     ref={el => nodeRefs.current[node.id] = el}
                     isHovered={hoveredNode === node.id}
                     isSelected={selectedNode === node.id}
                     onHover={setHoveredNode} onClick={setSelectedNode}/>
          ))}
        </TierBand>
      ))}
    </div>
  );
};
</script>
```

**Phase 3: Tier Configuration**

```javascript
const SEG_TIER_CONFIG = {
  1: { label: 'TIER 1 — INDEPENDENT TABLES', color: '#3B82F6', bg: 'rgba(59,130,246,0.06)' },
  2: { label: 'TIER 2 — DEPENDENT TABLES (FK CHAINS)', color: '#EAB308', bg: 'rgba(234,179,8,0.06)' },
  3: { label: 'TIER 3 — DEEP DEPENDENCIES', color: '#A855F7', bg: 'rgba(168,85,247,0.06)' },
  // Medallion tiers (added during conform step)
  10: { label: 'BRONZE — RAW INGESTION', color: '#CD7F32', bg: 'rgba(205,127,50,0.06)' },
  11: { label: 'SILVER — CLEANSED', color: '#C0C0C0', bg: 'rgba(192,192,192,0.06)' },
  12: { label: 'GOLD — AGGREGATED', color: '#FFD700', bg: 'rgba(255,215,0,0.06)' },
  // Validation tier (added during validate step)
  20: { label: 'VALIDATION — CONFIDENCE SCORE', color: '#21C354', bg: 'rgba(33,195,84,0.06)' },
};

// NiFi-specific tiers (when source is NiFi)
const NIFI_TIER_CONFIG = {
  1: { label: 'TIER 1 — INGESTION', color: '#3B82F6', bg: 'rgba(59,130,246,0.06)' },
  2: { label: 'TIER 2 — ROUTING & TRANSFORMATION', color: '#EAB308', bg: 'rgba(234,179,8,0.06)' },
  3: { label: 'TIER 3 — PROCESSING', color: '#A855F7', bg: 'rgba(168,85,247,0.06)' },
  4: { label: 'TIER 4 — OUTPUT & DELIVERY', color: '#21C354', bg: 'rgba(33,195,84,0.06)' },
};
```

**Phase 4: Connection Rendering (SVG Bezier)**

From Lumen_Retro's approach:

```javascript
function buildPath(fromRect, toRect, containerRect, scrollTop, scrollLeft) {
  const fromX = fromRect.left + fromRect.width/2 - containerRect.left + scrollLeft;
  const fromY = fromRect.top + fromRect.height - containerRect.top + scrollTop;
  const toX = toRect.left + toRect.width/2 - containerRect.left + scrollLeft;
  const toY = toRect.top - containerRect.top + scrollTop;

  const dy = toY - fromY;
  const cp = Math.max(Math.abs(dy) * 0.35, 30);
  const cpx = (toX - fromX) * 0.15;

  return `M${fromX},${fromY} C${fromX+cpx},${fromY+cp} ${toX-cpx},${toY-cp} ${toX},${toY}`;
}
```

**Phase 5: Interaction**

- **Hover**: Highlight node + connected lines, dim others
- **Click**: Select node, show detail panel (columns, types, FKs, stats)
- **Connection hover**: Show relationship details
- **Auto-layout**: Nodes per tier centered with flexbox wrap

### 4.5 When Diagram is Generated

The tier diagram appears in **Step 2 (Blueprint)** after assembly, and **evolves** through subsequent steps:

| Step | Diagram State |
|------|--------------|
| 2. Blueprint | Table dependency tiers with FK connections |
| 3. Generate | Same + row count badges on each node |
| 4. Conform | Adds Bronze → Silver → Gold tiers below |
| 5. Validate | Adds validation tier with confidence score, color-codes all nodes by health |

---

## 5. Environment Recreation Capabilities

### 5.1 What "Recreating Conditions" Means

For each source system, the demo should not just parse the schema — it should **recreate the operational environment** including:

| Source | Environment Details to Recreate |
|--------|-------------------------------|
| NiFi | Full flow topology: processors, connections, controller services, scheduling, back-pressure |
| Informatica | Source-to-target mappings, transformations, session configs |
| Talend | Job structure, component connections, schema propagation |
| dbt | Model DAG, ref dependencies, test coverage, materializations |
| Any SQL | Table relationships, constraints, indexes, partitioning |

### 5.2 Implementation

**NiFi Environment Recreation:**
```javascript
// After parsing NiFi XML, build the full environment model
function recreateNiFiEnvironment(nifiData) {
  return {
    flow_topology: buildFlowDAG(nifiData.processors, nifiData.connections),
    process_groups: buildGroupHierarchy(nifiData.processGroups),
    scheduling_profile: analyzeScheduling(nifiData.processors),
    back_pressure_map: analyzeBackPressure(nifiData.connections),
    controller_dependencies: mapServiceDependencies(nifiData.controllerServices, nifiData.processors),
    sql_lineage: extractSQLLineage(nifiData.sqlTables, nifiData.processors),
    // Visual: which processors are sources, transforms, sinks
    processor_roles: classifyProcessors(nifiData.processors)
  };
}
```

**NiFi Processor Classification (for tiering):**
```javascript
const NIFI_ROLE_MAP = {
  // Sources (Tier 1)
  'GetFile': 'source', 'GetHTTP': 'source', 'GetSFTP': 'source',
  'ConsumeKafka': 'source', 'ListenHTTP': 'source', 'QueryDatabaseTable': 'source',
  'GenerateFlowFile': 'source', 'GetHDFS': 'source', 'ListS3': 'source',
  // Routing (Tier 2)
  'RouteOnAttribute': 'route', 'RouteOnContent': 'route', 'DistributeLoad': 'route',
  'ControlRate': 'route',
  // Transform (Tier 2-3)
  'UpdateAttribute': 'transform', 'JoltTransformJSON': 'transform',
  'ReplaceText': 'transform', 'ConvertRecord': 'transform',
  'SplitRecord': 'transform', 'MergeContent': 'transform',
  'ExecuteScript': 'transform', 'ExecuteStreamCommand': 'transform',
  // Processing (Tier 3)
  'ExecuteSQL': 'process', 'PutDatabaseRecord': 'process',
  'LookupAttribute': 'process', 'LookupRecord': 'process',
  // Sinks (Tier 4)
  'PutFile': 'sink', 'PutHDFS': 'sink', 'PutS3Object': 'sink',
  'PutSQL': 'sink', 'PutKafka': 'sink', 'PutEmail': 'sink',
  // Utility
  'LogMessage': 'utility', 'LogAttribute': 'utility',
  'Wait': 'utility', 'Notify': 'utility',
};
```

### 5.3 SQL Lineage Extraction

For any source with embedded SQL, trace data lineage:

```javascript
function extractSQLLineage(sqlContent) {
  const lineage = [];
  // Extract: source tables (FROM, JOIN), target tables (INTO, UPDATE), columns (SELECT list)
  // Build: source → transform → target flow
  // Handle: CTEs, subqueries, UNION, variable references
  return lineage;
}
```

---

## 6. Summary: Implementation Priority

### Phase 1 — Critical (Parser Resilience)
1. Implement `cleanInput()` — BOM, encoding, whitespace normalization
2. Implement `smartParse()` — cascading parse with ranked candidates
3. Implement `validateParseResult()` — quality scoring
4. Fix DDL parser — strip comments/strings before depth counting
5. Fix CSV parser — proper quote-aware field splitting
6. Add try/catch wrappers to all parsers
7. Implement `bruteForceExtract()` — last-resort extraction

### Phase 2 — High (Per-Parser Fixes)
8. Fix all XML parsers — namespace handling, case normalization
9. Fix YAML parser — tab normalization, inline comment handling
10. Fix PySpark/Scala — nested type handling, generic syntax
11. Fix Protobuf — nested messages, map types, comment stripping
12. Fix dbt — CTE handling, window function stripping
13. Fix Avro — union types at record level, nested records

### Phase 3 — Blueprint Tier Diagram
14. Add React + Babel CDN to `index.html`
15. Build `TierDiagram` component with node rendering
16. Build SVG connection layer with Bezier curves
17. Build tier data model from blueprint (topological sort)
18. Add hover/click interaction
19. Add NiFi-specific tier classification
20. Progressive diagram: evolves through Steps 2-5

### Phase 4 — Environment Recreation
21. NiFi flow topology reconstruction
22. NiFi processor role classification
23. SQL lineage extraction
24. Environment summary dashboard in Blueprint step
25. Export environment model as JSON

---

## 7. Testing Matrix

Every parser must be tested with these scenarios:

| Scenario | Input | Expected |
|----------|-------|----------|
| Happy path | Well-formed input | Full parse, 0 warnings |
| BOM + CRLF | `\uFEFF` prefix, `\r\n` endings | Same result as happy path |
| Extra whitespace | Leading/trailing spaces, blank lines | Same result |
| Wrong extension | `.txt` file with DDL content | Detects DDL from content |
| Mixed case | `CREATE table`, `<TABLE>`, `<Source>` | Case-insensitive match |
| Partial validity | 5 tables, 2 malformed | 3 tables + warnings |
| Empty file | 0 bytes | Friendly error message |
| Massive file | 100K+ lines | Parses within 5 seconds |
| Binary garbage | Random bytes | "Unable to parse" with suggestions |
| Wrong format | JSON file with `.xml` extension | Detects JSON from content |
| Nested structures | NiFi with 5 levels of processGroups | Recursive extraction |
| SQL in properties | NiFi with embedded CREATE TABLE | DDL extracted from properties |
| Unicode content | Table names with accents, CJK chars | Preserved correctly |
