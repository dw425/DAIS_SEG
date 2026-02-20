/**
 * core/event-bus.js — Simple pub/sub event bus
 *
 * Decouples UI components from pipeline steps so they can communicate
 * without direct function references.
 */

class EventBus {
  constructor() {
    /** @type {Map<string, Set<Function>>} */
    this._handlers = new Map();
  }

  /**
   * Subscribe to an event.
   * @param {string} event
   * @param {Function} handler
   * @returns {Function} unsubscribe
   */
  on(event, handler) {
    if (!this._handlers.has(event)) {
      this._handlers.set(event, new Set());
    }
    this._handlers.get(event).add(handler);
    return () => this.off(event, handler);
  }

  /**
   * Subscribe to an event once, then auto-remove.
   * @param {string} event
   * @param {Function} handler
   * @returns {Function} unsubscribe (in case you want to remove before it fires)
   */
  once(event, handler) {
    const wrapper = (...args) => {
      this.off(event, wrapper);
      handler(...args);
    };
    return this.on(event, wrapper);
  }

  /**
   * Remove a specific handler for an event.
   * @param {string} event
   * @param {Function} handler
   */
  off(event, handler) {
    const set = this._handlers.get(event);
    if (set) {
      set.delete(handler);
      if (set.size === 0) this._handlers.delete(event);
    }
  }

  /**
   * Emit an event to all subscribers.
   * @param {string} event
   * @param  {...any} args
   */
  emit(event, ...args) {
    const set = this._handlers.get(event);
    if (!set) return;
    for (const handler of set) {
      try {
        handler(...args);
      } catch (err) {
        console.error(`[EventBus] handler for "${event}" threw:`, err);
      }
    }
  }

  /**
   * Remove all handlers (useful for teardown / tests).
   * @param {string} [event] — if omitted, clears everything
   */
  clear(event) {
    if (event) {
      this._handlers.delete(event);
    } else {
      this._handlers.clear();
    }
  }
}

/** Singleton instance shared across the application. */
const bus = new EventBus();

export { EventBus };
export default bus;
