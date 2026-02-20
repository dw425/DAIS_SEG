// ================================================================
// nel/variable-registry.js — Parse NiFi variable registries and
// parameter contexts, resolve variable references in text
// Extracted from monolith lines 3428-3462
// ================================================================

/**
 * Parse NiFi variable registry and parameter context definitions
 * from raw XML content embedded in the parsed NiFi data.
 *
 * Extracts:
 *   - <variable name="..." value="..."/> elements
 *   - <parameter><name>...</name><value>...</value></parameter> elements
 *
 * @param {Object} nifiData - Parsed NiFi data with optional _rawXml property
 * @returns {Object} Map of variable/parameter names to values
 */
export function parseVariableRegistry(nifiData) {
  const vars = {};
  // Parse from XML variable elements
  if (nifiData._rawXml) {
    const varMatches = (nifiData._rawXml.match(/<variable\s+(?:name="[^"]+"\s+value="[^"]*"|value="[^"]*"\s+name="[^"]+")\s*\/>/g)) || [];
    varMatches.forEach(m => {
      const nameMatch = m.match(/name="([^"]+)"/);
      const valMatch = m.match(/value="([^"]*)"/);
      if (nameMatch && valMatch) {
        vars[nameMatch[1]] = valMatch[1];
      }
    });
    // Parse parameter contexts
    const paramMatches = nifiData._rawXml.match(/<parameter>\s*<name>([^<]+)<\/name>\s*<value>([^<]*)<\/value>/g) || [];
    paramMatches.forEach(m => {
      const nameMatch = m.match(/<name>([^<]+)<\/name>/);
      const valMatch = m.match(/<value>([^<]*)<\/value>/);
      if (!nameMatch || !valMatch) return;  // skip malformed entries
      vars[nameMatch[1]] = valMatch[1];
    });
  }
  return vars;
}

/**
 * Resolve variable and parameter references in a text string.
 * Replaces ${var_name} and #{param_name} with actual values from the registry.
 *
 * @param {string} text - Text containing variable references
 * @param {Object} varRegistry - Map of variable names to values
 * @returns {string} Text with variables resolved
 */
export function resolveVariables(text, varRegistry) {
  if (!text || !varRegistry || Object.keys(varRegistry).length === 0) return text;
  let resolved = text;
  // Replace ${var_name} with actual values
  for (const [name, value] of Object.entries(varRegistry)) {
    // Escape $ in replacement string to prevent $&, $1 etc. being interpreted as backreferences
    const safeValue = String(value).replace(/\$/g, '$$$$');
    resolved = resolved.replace(new RegExp('\\$\\{' + name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\}', 'g'), safeValue);
    // Also replace #{param_name} (parameter context syntax)
    resolved = resolved.replace(new RegExp('#\\{' + name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\}', 'g'), safeValue);
  }
  return resolved;
}
