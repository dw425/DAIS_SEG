/**
 * analyzers/attribute-flow.js — Track NiFi FlowFile attributes through the pipeline
 *
 * Analyzes processor properties to determine which attributes are:
 * - Created (UpdateAttribute, EvaluateJsonPath, ExtractText, etc.)
 * - Read/Referenced (via ${attr} NiFi EL in any property)
 * - Modified (UpdateAttribute overwriting existing attributes)
 *
 * @module analyzers/attribute-flow
 */

// Properties that are NiFi config, not user-created attributes
const INTERNAL_PROPS = new Set([
  'Destination', 'Return Type', 'Path Not Found Behavior', 'Null Value Representation',
  'Character Set', 'Maximum Buffer Size', 'Maximum Capture Group Length',
  'Enable Canonical Equivalence', 'Enable Case-insensitive Matching',
  'Permit Whitespace and Comments', 'Include Zero Capture Groups',
  'Delete Attributes Expression', 'Store State', 'Stateful Variables Initial Value',
  'Canonical Value Lookup Cache Size',
]);

// Processor types that create attributes from their property keys
const ATTRIBUTE_CREATORS = new Set([
  'UpdateAttribute', 'PutAttribute',
  'EvaluateJsonPath', 'EvaluateXPath', 'EvaluateXQuery',
  'ExtractText', 'ExtractGrok', 'ExtractHL7Attributes',
]);

/**
 * Analyze attribute flow across all processors in the flow.
 *
 * @param {Array} processors - parsed processor array with .properties, .name, .type
 * @param {Array} connections - parsed connections array with .sourceName, .destinationName
 * @returns {{
 *   attributeMap: Object<string, { creators: string[], readers: string[], modifiers: string[] }>,
 *   processorAttributes: Object<string, { creates: string[], reads: string[] }>,
 *   attributeLineage: Object<string, Array<{proc: string, action: 'create'|'read'}>>
 * }}
 */
export function analyzeAttributeFlow(processors, connections) {
  const attributeMap = {};
  const processorAttrs = {};

  function ensureAttr(name) {
    if (!attributeMap[name]) attributeMap[name] = { creators: [], readers: [], modifiers: [] };
    return attributeMap[name];
  }
  function ensureProc(name) {
    if (!processorAttrs[name]) processorAttrs[name] = { creates: [], reads: [] };
    return processorAttrs[name];
  }

  (processors || []).forEach(p => {
    if (!p || !p.name) return;
    const props = p.properties || {};
    const procInfo = ensureProc(p.name);

    // 1. Extract attributes CREATED by this processor
    if (ATTRIBUTE_CREATORS.has(p.type)) {
      Object.keys(props).forEach(k => {
        if (INTERNAL_PROPS.has(k)) return;
        // Skip NiFi-internal property names (contain spaces and look like config)
        if (k.startsWith('nifi-') || k.startsWith('Record ')) return;

        const attr = ensureAttr(k);
        // If attribute was already created upstream, this is a modification
        if (attr.creators.length > 0 && (p.type === 'UpdateAttribute' || p.type === 'PutAttribute')) {
          if (!attr.modifiers.includes(p.name)) attr.modifiers.push(p.name);
        } else {
          if (!attr.creators.includes(p.name)) attr.creators.push(p.name);
        }
        if (!procInfo.creates.includes(k)) procInfo.creates.push(k);
      });
    }

    // 2. Extract attributes READ by this processor (NiFi EL references: ${attrName})
    Object.values(props).forEach(v => {
      if (!v || typeof v !== 'string' || !v.includes('${')) return;
      const refs = v.match(/\$\{([^}:]+)/g);
      if (!refs) return;
      refs.forEach(ref => {
        const raw = ref.slice(2).trim();
        // Extract attribute name (before any EL function calls)
        const attrName = raw.split(':')[0].split('.')[0].trim();
        // Skip NiFi built-in functions that aren't attribute names
        if (!attrName || attrName.includes('(') || attrName.length > 80) return;
        if (/^(now|nextInt|random|UUID|uuid|hostname|ip|literal|thread|entryDate)$/i.test(attrName)) return;

        const attr = ensureAttr(attrName);
        if (!attr.readers.includes(p.name)) attr.readers.push(p.name);
        if (!procInfo.reads.includes(attrName)) procInfo.reads.push(attrName);
      });
    });
  });

  // 3. Build lineage: ordered sequence of create/read/modify for each attribute
  const attributeLineage = {};
  Object.entries(attributeMap).forEach(([attrName, entry]) => {
    const lineage = [];
    entry.creators.forEach(p => lineage.push({ proc: p, action: 'create' }));
    entry.modifiers.forEach(p => lineage.push({ proc: p, action: 'modify' }));
    entry.readers.forEach(p => lineage.push({ proc: p, action: 'read' }));
    if (lineage.length > 0) {
      attributeLineage[attrName] = lineage;
    }
  });

  return { attributeMap, processorAttributes: processorAttrs, attributeLineage };
}

/**
 * Get attributes created by processors in a specific group.
 *
 * @param {Array} groupProcessors - processor objects in the group
 * @param {Object} processorAttributes - from analyzeAttributeFlow()
 * @returns {string[]} - attribute names created by this group
 */
export function getGroupAttrCreates(groupProcessors, processorAttributes) {
  const attrs = new Set();
  (groupProcessors || []).forEach(p => {
    const pa = processorAttributes[p.name];
    if (pa) pa.creates.forEach(a => attrs.add(a));
  });
  return [...attrs];
}

/**
 * Get attributes read by processors in a specific group.
 *
 * @param {Array} groupProcessors - processor objects in the group
 * @param {Object} processorAttributes - from analyzeAttributeFlow()
 * @returns {string[]} - attribute names read by this group
 */
export function getGroupAttrReads(groupProcessors, processorAttributes) {
  const attrs = new Set();
  (groupProcessors || []).forEach(p => {
    const pa = processorAttributes[p.name];
    if (pa) pa.reads.forEach(a => attrs.add(a));
  });
  return [...attrs];
}
