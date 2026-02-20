/**
 * analyzers/scheduling-analyzer.js — CRON parsing and timer scheduling analysis
 *
 * Synthesized from scheduling logic scattered across the monolith:
 * - CRON expression regex from constants/_RE.cronExpr (line 1127)
 * - Scheduling capture from processor scanning (line 1503-1507)
 * - Timer-driven / CRON-driven strategy extraction from parsed processors
 *
 * @module analyzers/scheduling-analyzer
 */

/**
 * CRON field labels for 5-field (standard) and 6-field (NiFi/Quartz) expressions.
 */
const CRON_FIELDS_5 = ['minute', 'hour', 'day-of-month', 'month', 'day-of-week'];
const CRON_FIELDS_6 = ['second', 'minute', 'hour', 'day-of-month', 'month', 'day-of-week'];

/**
 * Month and day-of-week names for human-readable output.
 */
const MONTH_NAMES = [null, 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const DOW_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

/**
 * Parse a single CRON field and return a human-readable description.
 *
 * @param {string} field — raw CRON field value (e.g. "5", "*", "1-5", "* /10")
 * @param {string} label — field label (e.g. "minute", "hour")
 * @returns {string}
 */
function describeCronField(field, label) {
  if (field === '*') return `every ${label}`;
  if (field === '?') return `any ${label}`;

  // Step syntax: */N or N/M
  if (field.includes('/')) {
    const [range, step] = field.split('/');
    if (range === '*') return `every ${step} ${label}s`;
    return `every ${step} ${label}s starting at ${range}`;
  }

  // Range: N-M
  if (field.includes('-') && !field.includes(',')) {
    const [start, end] = field.split('-');
    if (label === 'day-of-week') {
      return `${DOW_NAMES[start] || start} through ${DOW_NAMES[end] || end}`;
    }
    if (label === 'month') {
      return `${MONTH_NAMES[start] || start} through ${MONTH_NAMES[end] || end}`;
    }
    return `${label}s ${start} through ${end}`;
  }

  // List: N,M,O
  if (field.includes(',')) {
    const parts = field.split(',');
    if (label === 'day-of-week') {
      return parts.map(p => DOW_NAMES[p] || p).join(', ');
    }
    if (label === 'month') {
      return parts.map(p => MONTH_NAMES[p] || p).join(', ');
    }
    return `${label}s ${parts.join(', ')}`;
  }

  // Specific value
  if (label === 'day-of-week') return DOW_NAMES[field] || `day ${field}`;
  if (label === 'month') return MONTH_NAMES[field] || `month ${field}`;
  return `at ${label} ${field}`;
}

/**
 * Parse a CRON expression and return a human-readable schedule description.
 *
 * Handles both 5-field (standard) and 6-field (Quartz/NiFi) expressions.
 *
 * @param {string} expr — CRON expression (e.g. "0 0 * * *" or "0 0 12 * * ?")
 * @returns {{ valid: boolean, description: string, fields: object }}
 */
export function analyzeCRON(expr) {
  if (!expr || typeof expr !== 'string') {
    return { valid: false, description: 'Invalid CRON expression', fields: {} };
  }

  const parts = expr.trim().split(/\s+/);
  if (parts.length < 5 || parts.length > 7) {
    return { valid: false, description: `Invalid CRON expression (${parts.length} fields, expected 5-6)`, fields: {} };
  }

  const is6Field = parts.length >= 6;
  const fieldLabels = is6Field ? CRON_FIELDS_6 : CRON_FIELDS_5;
  const fields = {};
  const descriptions = [];

  for (let i = 0; i < fieldLabels.length && i < parts.length; i++) {
    fields[fieldLabels[i]] = parts[i];
    if (parts[i] !== '*' && parts[i] !== '?') {
      descriptions.push(describeCronField(parts[i], fieldLabels[i]));
    }
  }

  const description = descriptions.length > 0
    ? descriptions.join(', ')
    : 'every minute (all wildcards)';

  return { valid: true, description, fields };
}

/**
 * Extract scheduling information from all processors.
 *
 * For each processor, captures:
 * - schedulingStrategy (TIMER_DRIVEN, CRON_DRIVEN, EVENT_DRIVEN)
 * - schedulingPeriod (e.g. "5 min", "0 sec", CRON expression)
 * - Parsed CRON details if CRON_DRIVEN
 *
 * @param {Array} processors — parsed processor array
 * @returns {{ timers: Array, cronSchedules: Array, eventDriven: Array, summary: object }}
 */
export function analyzeTimers(processors) {
  const timers = [];
  const cronSchedules = [];
  const eventDriven = [];

  (processors || []).forEach(p => {
    const strategy = p.schedulingStrategy || 'TIMER_DRIVEN';
    const period = p.schedulingPeriod || '0 sec';

    if (strategy === 'CRON_DRIVEN') {
      const parsed = analyzeCRON(period);
      cronSchedules.push({
        processor: p.name,
        type: p.type,
        group: p.group || '(root)',
        expression: period,
        ...parsed,
      });
    } else if (strategy === 'EVENT_DRIVEN') {
      eventDriven.push({
        processor: p.name,
        type: p.type,
        group: p.group || '(root)',
      });
    } else {
      // TIMER_DRIVEN (default)
      timers.push({
        processor: p.name,
        type: p.type,
        group: p.group || '(root)',
        period,
        isZero: period === '0 sec' || period === '0 ms' || period === '0',
      });
    }
  });

  return {
    timers,
    cronSchedules,
    eventDriven,
    summary: {
      totalProcessors: (processors || []).length,
      timerDriven: timers.length,
      cronDriven: cronSchedules.length,
      eventDrivenCount: eventDriven.length,
      zeroPeriodCount: timers.filter(t => t.isZero).length,
    },
  };
}
