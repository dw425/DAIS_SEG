/**
 * generators/placeholder-resolver.js — Notebook Placeholder Resolution
 *
 * Replaces placeholder tokens in generated notebook code with actual
 * Databricks configuration values.
 *
 * Extracted from index.html lines 7283-7292.
 *
 * NOTE: This is a re-export from core/config.js where the canonical
 * implementation lives (resolveNotebookPlaceholders). This module
 * exists for backward compatibility and direct generator imports.
 *
 * @module generators/placeholder-resolver
 */

export { resolveNotebookPlaceholders } from '../core/config.js';
