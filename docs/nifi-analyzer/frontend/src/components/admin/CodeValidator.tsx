import React, { useState } from 'react';

interface ValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

function validatePythonSyntax(code: string): ValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  if (!code.trim()) {
    errors.push('Code is empty');
    return { valid: false, errors, warnings };
  }

  // Basic checks (real validation would happen server-side)
  const lines = code.split('\n');
  let openParens = 0;
  let openBrackets = 0;
  let openBraces = 0;

  lines.forEach((line, i) => {
    const lineNum = i + 1;
    // Check indentation consistency
    if (line.match(/^\t/) && lines.some((l) => l.match(/^ /))) {
      warnings.push(`Line ${lineNum}: Mixed tabs and spaces`);
    }
    // Count brackets
    for (const ch of line) {
      if (ch === '(') openParens++;
      if (ch === ')') openParens--;
      if (ch === '[') openBrackets++;
      if (ch === ']') openBrackets--;
      if (ch === '{') openBraces++;
      if (ch === '}') openBraces--;
    }
    // Check for common issues
    if (line.match(/print\s+[^(]/)) {
      warnings.push(`Line ${lineNum}: print without parentheses (Python 2 style)`);
    }
    if (line.match(/except\s*:/)) {
      warnings.push(`Line ${lineNum}: Bare except clause`);
    }
  });

  if (openParens !== 0) errors.push(`Unmatched parentheses (${openParens > 0 ? 'missing )' : 'extra )'})`);
  if (openBrackets !== 0) errors.push(`Unmatched brackets (${openBrackets > 0 ? 'missing ]' : 'extra ]'})`);
  if (openBraces !== 0) errors.push(`Unmatched braces (${openBraces > 0 ? 'missing }' : 'extra }'})`);

  return { valid: errors.length === 0, errors, warnings };
}

export default function CodeValidator() {
  const [code, setCode] = useState('');
  const [result, setResult] = useState<ValidationResult | null>(null);

  const handleValidate = () => {
    setResult(validatePythonSyntax(code));
  };

  return (
    <div className="space-y-4">
      {/* Textarea */}
      <div>
        <label className="text-sm text-gray-400 mb-1 block">Paste code to validate:</label>
        <textarea
          value={code}
          onChange={(e) => setCode(e.target.value)}
          rows={12}
          className="w-full rounded-lg bg-gray-950 border border-border p-3 text-sm text-gray-200 font-mono
            placeholder-gray-600 focus:outline-none focus:border-gray-600 resize-y"
          placeholder="# Paste Python/Spark code here..."
        />
      </div>

      {/* Validate button */}
      <div className="flex items-center gap-3">
        <button
          onClick={handleValidate}
          disabled={!code.trim()}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
        >
          Validate
        </button>
        <span className="text-xs text-gray-600">{code.split('\n').length} lines</span>
        {result && (
          <span className={`ml-auto text-sm font-medium ${result.valid ? 'text-green-400' : 'text-red-400'}`}>
            {result.valid ? 'Valid' : 'Issues found'}
          </span>
        )}
      </div>

      {/* Results */}
      {result && (
        <div className="space-y-3">
          {result.errors.length > 0 && (
            <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-4">
              <h4 className="text-sm font-medium text-red-400 mb-2">Errors ({result.errors.length})</h4>
              <ul className="space-y-1">
                {result.errors.map((e, i) => (
                  <li key={i} className="text-xs text-red-300/80 flex items-start gap-2">
                    <span className="text-red-400 mt-0.5">&#x2716;</span>
                    {e}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {result.warnings.length > 0 && (
            <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-4">
              <h4 className="text-sm font-medium text-amber-400 mb-2">Warnings ({result.warnings.length})</h4>
              <ul className="space-y-1">
                {result.warnings.map((w, i) => (
                  <li key={i} className="text-xs text-amber-300/80 flex items-start gap-2">
                    <span className="text-amber-400 mt-0.5">&#x26A0;</span>
                    {w}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {result.valid && result.warnings.length === 0 && (
            <div className="rounded-lg border border-green-500/30 bg-green-500/5 p-4 text-center">
              <span className="text-sm text-green-400">No issues found</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
