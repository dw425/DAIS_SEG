// ================================================================
// nel/java-date-format.js — Java date format -> Python strftime
// Extracted from monolith lines 3414-3418
// ================================================================

/**
 * Convert a Java SimpleDateFormat pattern to Python strftime format.
 *
 * Supported tokens:
 *   yyyy -> %Y, MM -> %m, dd -> %d, HH -> %H, mm -> %M,
 *   ss -> %S, SSS -> %f, E+ -> %a, Z -> %z
 *
 * @param {string} fmt - Java date format pattern
 * @returns {string} Python strftime format pattern
 */
export function javaDateToPython(fmt) {
  return fmt.replace(/yyyy/g, '%Y').replace(/MM/g, '%m').replace(/dd/g, '%d')
    .replace(/HH/g, '%H').replace(/mm/g, '%M').replace(/ss/g, '%S')
    .replace(/SSS/g, '%f').replace(/E+/g, '%a').replace(/Z/g, '%z');
}
