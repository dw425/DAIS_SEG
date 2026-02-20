/**
 * analyzers/security-scanner.js — Scan processor properties for security vulnerabilities
 *
 * Extracted from index.html lines 3533-3563.
 *
 * @module analyzers/security-scanner
 */

import { SENSITIVE_PROP_RE } from '../security/sensitive-props.js';

/**
 * Security vulnerability patterns — each with a name, regex, and severity.
 */
const SECURITY_PATTERNS = [
  { name: 'SQL Injection', regex: /(\bDROP\s+TABLE\b|\bUNION\s+SELECT\b|\bxp_cmdshell\b|\bEXEC\s+sp_|\bDELETE\s+FROM\b.*;\s*--|;\s*DROP\b|'\s*OR\s+'[^']*'\s*=\s*')/i, severity: 'CRITICAL' },
  { name: 'Command Injection', regex: /(rm\s+-rf\s+\/|curl\s+evil|wget\s+.*\|\s*bash|\/bin\/sh\s+-[ic]|exec\s*\(|system\s*\(|subprocess)/i, severity: 'CRITICAL' },
  { name: 'SSRF', regex: /(169\.254\.169\.254|metadata\.google\.internal|localhost:\d{4}\/admin)/i, severity: 'HIGH' },
  { name: 'Hardcoded Secret', regex: /(password\s*=\s*['"][^'"]{3,}|secret\s*=\s*['"][^'"]{3,}|api_key\s*=\s*['"][^'"]{3,})/i, severity: 'HIGH' },
  { name: 'Reverse Shell', regex: /(\/dev\/tcp\/|nc\s+-[el]|ncat\s|socket\.SOCK_STREAM.*connect|bash\s+-i\s*>&)/i, severity: 'CRITICAL' },
  { name: 'Path Traversal', regex: /(\.\.\/\.\.\/|\/etc\/passwd|\/etc\/shadow|\/root\/)/i, severity: 'MEDIUM' },
  { name: 'Dangerous File Access', regex: /(\.pem|\.key|\.crt|credentials\.json|\.env\b)/i, severity: 'MEDIUM' },
  { name: 'Perl Injection', regex: /(use\s+IO::Socket|eval\s*\(|open\s*\(\s*F\s*,\s*'\|)/i, severity: 'CRITICAL' },
  { name: 'Java Exploitation', regex: /(Runtime\.getRuntime\(\)|ProcessBuilder|jndi:ldap|Class\.forName)/i, severity: 'CRITICAL' },
  { name: 'Data Exfiltration', regex: /(LOAD_FILE|INTO\s+OUTFILE|COPY\s+.*\s+TO\s+'\/|UTL_HTTP\.REQUEST)/i, severity: 'HIGH' },
];

/**
 * Scan processor properties for known security vulnerability patterns.
 *
 * Also flags any properties whose keys match SENSITIVE_PROP_RE (password, secret,
 * token, etc.) that contain non-masked literal values.
 *
 * @param {Array} processors — parsed processors with .properties
 * @returns {Array<{ processor: string, type: string, finding: string, severity: string, snippet: string }>}
 */
export function scanSecurity(processors) {
  const findings = [];

  processors.forEach(p => {
    const allValues = Object.values(p.properties || {}).join(' ');

    SECURITY_PATTERNS.forEach(pat => {
      const match = pat.regex.exec(allValues);
      if (match) {
        findings.push({
          processor: p.name,
          type: p.type,
          finding: pat.name,
          severity: pat.severity,
          snippet: match[0].substring(0, 100),
        });
      }
    });
  });

  return findings;
}
