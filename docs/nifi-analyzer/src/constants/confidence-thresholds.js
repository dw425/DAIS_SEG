/**
 * constants/confidence-thresholds.js — Shared confidence threshold constants
 *
 * Centralized thresholds used across validators and reporters to ensure
 * consistent classification of mapping confidence levels.
 *
 * @module constants/confidence-thresholds
 */

export const CONFIDENCE_THRESHOLDS = Object.freeze({
  MAPPED: 0.7,      // Minimum confidence to consider a processor "mapped"
  EXACT: 0.85,      // Minimum confidence for "exact match" in comparison
  PARTIAL: 0.4,     // Below this = unmapped
});
