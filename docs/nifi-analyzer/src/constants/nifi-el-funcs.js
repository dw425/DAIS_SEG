// NiFi Expression Language built-in function names
export const NIFI_EL_FUNCS = new Set([
  'now','nextInt','UUID','hostname','IP','literal','thread','format',
  'toDate','substring','substringBefore','substringAfter','replace',
  'replaceAll','replaceFirst','replaceEmpty','replaceNull','toUpper',
  'toLower','trim','length','isEmpty','equals','equalsIgnoreCase',
  'contains','startsWith','endsWith','append','prepend','plus','minus',
  'multiply','divide','mod','gt','ge','lt','le','and','or','not',
  'ifElse','toString','toNumber','math','getStateValue','count',
  'padLeft','padRight','escapeJson','escapeXml','escapeCsv',
  'unescapeJson','unescapeXml','urlEncode','urlDecode','base64Encode',
  'base64Decode','toRadix','jsonPath','jsonPathDelete','jsonPathAdd',
  'jsonPathSet','jsonPathPut'
]);
