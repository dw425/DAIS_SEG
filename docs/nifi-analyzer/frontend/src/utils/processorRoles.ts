export const ROLE_COLORS: Record<string, string> = {
  source: '#3B82F6',
  transform: '#22C55E',
  process: '#A855F7',
  route: '#EAB308',
  sink: '#EF4444',
  utility: '#6B7280',
};

export const ROLE_PATTERNS: [RegExp, string][] = [
  [/^(Get|Consume|Listen|Fetch|Tail|List|Query|Scan|Select|Generate)/i, 'source'],
  [/^(Put|Publish|Send|Post|Insert|Write|Delete|Store)/i, 'sink'],
  [/^(Route|Distribute|Detect|Funnel)/i, 'route'],
  [/^(Execute|Invoke|Handle)/i, 'process'],
  [/^(Log|Debug|Count|Wait|Notify|Control|Monitor)/i, 'utility'],
  [/Convert|Replace|Update|Jolt|Extract|Evaluate|Transform|Compress|Encrypt|Lookup|Validate|Split|Merge/i, 'transform'],
];

export function classifyRole(type: string): string {
  for (const [re, role] of ROLE_PATTERNS) if (re.test(type)) return role;
  return 'transform';
}
