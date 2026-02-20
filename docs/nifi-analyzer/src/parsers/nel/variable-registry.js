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
    const varMatches = nifiData._rawXml.match(/<variable\s+name="([^"]+)"\s+value="([^"]*)"\s*\/>/g) || [];
    varMatches.forEach(m => {
      const name = m.match(/name="([^"]+)"/)[1];
      const val = m.match(/value="([^"]*)"/)[1];
      vars[name] = val;
    });
    // Parse parameter contexts
    const paramMatches = nifiData._rawXml.match(/<parameter>\s*<name>([^<]+)<\/name>\s*<value>([^<]*)<\/value>/g) || [];
    paramMatches.forEach(m => {
      const name = m.match(/<name>([^<]+)<\/name>/)[1];
      const val = m.match(/<value>([^<]*)<\/value>/);
      if (val) vars[name] = val[1];
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
    resolved = resolved.replace(new RegExp('\\$\\{' + name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\}', 'g'), value);
    // Also replace #{param_name} (parameter context syntax)
    resolved = resolved.replace(new RegExp('#\\{' + name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\}', 'g'), value);
  }
  return resolved;
}
