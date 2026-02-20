/**
 * analyzers/phi-detector.js — Detect PHI/HIPAA-relevant fields in processor properties
 *
 * Extracted from index.html lines 3569-3602.
 *
 * @module analyzers/phi-detector
 */

/**
 * PHI field patterns — regex + human-readable category label.
 */
export const _PHI_PATTERNS = [
  { pattern: /\b(ssn|social.?security|ss_num)\b/i, field: 'SSN' },
  { pattern: /\b(mrn|medical.?record|patient.?id|patient_number)\b/i, field: 'MRN' },
  { pattern: /\b(dob|date.?of.?birth|birth.?date|birthdate)\b/i, field: 'DOB' },
  { pattern: /\b(patient|patient.?name|first.?name|last.?name|full.?name)\b/i, field: 'Patient Name' },
  { pattern: /\b(diagnosis|icd.?\d{1,2}|cpt.?code|procedure.?code|drg)\b/i, field: 'Diagnosis/Code' },
  { pattern: /\b(phone|fax|email|address|zip.?code|street)\b/i, field: 'Contact Info' },
  { pattern: /\b(insurance|policy.?number|member.?id|subscriber)\b/i, field: 'Insurance' },
  { pattern: /\b(prescription|medication|rx_|drug.?name|ndc)\b/i, field: 'Medication' },
  { pattern: /\b(lab.?result|test.?result|vital|blood.?pressure|heart.?rate)\b/i, field: 'Clinical Data' },
  { pattern: /\b(account.?number|billing|charge|payment)\b/i, field: 'Financial/Billing' },
];

/**
 * Detect PHI/HIPAA-relevant fields in a processor's properties.
 *
 * Scans both property keys and values against _PHI_PATTERNS.
 * Deduplicates results by key.
 *
 * @param {object} props — processor property key/value map
 * @returns {Array<{ key: string, category: string }>}
 */
export function detectPHIFields(props) {
  const phiFields = [];
  const allText = JSON.stringify(props || {}).toLowerCase();

  _PHI_PATTERNS.forEach(p => {
    if (p.pattern.test(allText)) {
      // Find which specific property keys match
      Object.keys(props || {}).forEach(k => {
        if (p.pattern.test(k)) phiFields.push({ key: k, category: p.field });
      });
      // Also check property values for column references
      Object.values(props || {}).forEach(v => {
        if (typeof v === 'string' && p.pattern.test(v)) {
          phiFields.push({ key: v.substring(0, 50), category: p.field });
        }
      });
    }
  });

  // Deduplicate by key
  const seen = new Set();
  return phiFields.filter(f => {
    if (seen.has(f.key)) return false;
    seen.add(f.key);
    return true;
  });
}
