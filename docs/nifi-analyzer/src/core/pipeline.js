/**
 * core/pipeline.js — PipelineOrchestrator
 *
 * Based on the sequential auto-run flow at index.html ~line 7462-7476.
 * Adds step-level locks to prevent race conditions when the user
 * triggers a re-parse while a previous pipeline is still running.
 */

import bus from './event-bus.js';
import { handleError, AppError } from './errors.js';

/**
 * @typedef {Object} PipelineStep
 * @property {string}   name    — human-readable step name (e.g. 'analyze')
 * @property {Function} run     — async function to execute
 * @property {string}   [tab]   — optional tab to switch to before running
 */

export class PipelineOrchestrator {
  constructor() {
    /** @type {PipelineStep[]} */
    this._steps = [];

    /** Currently executing run ID (incremented on each execute()) */
    this._runId = 0;

    /** True while a pipeline run is in progress */
    this._running = false;

    /** Set of step names currently locked (in-flight) */
    this._locks = new Set();
  }

  /**
   * Register an ordered list of pipeline steps.
   * @param {PipelineStep[]} steps
   */
  register(steps) {
    this._steps = steps.map((s, i) => ({ ...s, order: i }));
  }

  /**
   * True when any run is in progress.
   */
  get running() {
    return this._running;
  }

  /**
   * Execute all registered steps sequentially.
   * If a previous run is still in progress, it is cancelled (its runId
   * becomes stale and remaining steps are skipped).
   *
   * @param {object} [opts]
   * @param {number} [opts.delayMs=150] — delay between steps (ms)
   * @returns {Promise<void>}
   */
  async execute({ delayMs = 150 } = {}) {
    // Increment runId to invalidate any in-flight pipeline
    const thisRun = ++this._runId;
    this._running = true;
    bus.emit('pipeline:start', { runId: thisRun });

    try {
      for (const step of this._steps) {
        // Check if this run has been superseded
        if (this._runId !== thisRun) {
          bus.emit('pipeline:cancelled', { runId: thisRun, at: step.name });
          return;
        }

        // Step-level lock to prevent concurrent execution of the same step
        if (this._locks.has(step.name)) {
          console.warn(`[pipeline] skipping locked step: ${step.name}`);
          continue;
        }

        this._locks.add(step.name);
        bus.emit('pipeline:step:start', { runId: thisRun, step: step.name });

        try {
          await step.run();
        } catch (err) {
          handleError(
            err instanceof AppError
              ? err
              : new AppError(`Pipeline step "${step.name}" failed`, {
                  code: 'PIPELINE_STEP_FAILED',
                  phase: step.name,
                  severity: 'high',
                  cause: err,
                })
          );
          bus.emit('pipeline:step:error', {
            runId: thisRun,
            step: step.name,
            error: err,
          });
          // Continue to next step — one failure should not block everything
        } finally {
          this._locks.delete(step.name);
          bus.emit('pipeline:step:done', { runId: thisRun, step: step.name });
        }

        // Delay between steps (mirrors the original setTimeout pattern)
        if (delayMs > 0 && this._runId === thisRun) {
          await new Promise((r) => setTimeout(r, delayMs));
        }
      }
    } finally {
      if (this._runId === thisRun) {
        this._running = false;
        bus.emit('pipeline:done', { runId: thisRun });
      }
    }
  }

  /**
   * Cancel the current pipeline run (if any).
   */
  cancel() {
    if (this._running) {
      this._runId++;
      this._running = false;
      bus.emit('pipeline:cancelled', { runId: this._runId });
    }
  }
}
