/**
 * utils/accessibility.js — Accessibility utilities
 *
 * New module providing ARIA helpers for interactive elements.
 *
 * FIX MED: Ensures ARIA roles/labels are applied consistently to all
 *          interactive elements across the application.
 */

/**
 * Set multiple ARIA attributes on an element in one call.
 *
 * @param {HTMLElement} el
 * @param {Object<string, string>} attrs — keys are attribute names
 *        (without the "aria-" prefix), values are strings.
 *        Special keys: "role" sets the role attribute directly.
 *
 * @example
 *   setAriaAttrs(button, { role: 'button', label: 'Close dialog', expanded: 'false' });
 *   // sets role="button" aria-label="Close dialog" aria-expanded="false"
 */
export function setAriaAttrs(el, attrs) {
  if (!el || !attrs) return;
  Object.entries(attrs).forEach(([key, value]) => {
    if (key === 'role') {
      el.setAttribute('role', value);
    } else {
      el.setAttribute(`aria-${key}`, String(value));
    }
  });
}

/**
 * Trap keyboard focus within a container element (e.g. a modal dialog).
 *
 * Returns a cleanup function that removes the trap when called.
 *
 * @param {HTMLElement} container
 * @returns {function} release — call to remove the focus trap
 */
export function trapFocus(container) {
  if (!container) return () => {};

  const FOCUSABLE = [
    'a[href]',
    'button:not([disabled])',
    'textarea:not([disabled])',
    'input:not([disabled]):not([type="hidden"])',
    'select:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
  ].join(', ');

  function getFocusable() {
    return Array.from(container.querySelectorAll(FOCUSABLE))
      .filter(el => el.offsetParent !== null); // visible only
  }

  function handleKeydown(e) {
    if (e.key !== 'Tab') return;

    const focusable = getFocusable();
    if (focusable.length === 0) {
      e.preventDefault();
      return;
    }

    const first = focusable[0];
    const last = focusable[focusable.length - 1];

    if (e.shiftKey) {
      if (document.activeElement === first) {
        e.preventDefault();
        last.focus();
      }
    } else {
      if (document.activeElement === last) {
        e.preventDefault();
        first.focus();
      }
    }
  }

  container.addEventListener('keydown', handleKeydown);

  // Focus the first focusable element on trap activation
  const focusable = getFocusable();
  if (focusable.length > 0) {
    focusable[0].focus();
  }

  return function release() {
    container.removeEventListener('keydown', handleKeydown);
  };
}

/**
 * Announce a message to screen readers via a live region.
 *
 * Creates (or reuses) an off-screen aria-live region and sets its content.
 * The region is cleared after a short delay so repeated identical messages
 * are still announced.
 *
 * @param {string} message
 * @param {'polite'|'assertive'} [priority='polite']
 */
export function announceToSR(message, priority = 'polite') {
  const REGION_ID = '__sr-announce';
  let region = document.getElementById(REGION_ID);

  if (!region) {
    region = document.createElement('div');
    region.id = REGION_ID;
    region.setAttribute('aria-live', priority);
    region.setAttribute('aria-atomic', 'true');
    region.setAttribute('role', 'status');
    // Visually hidden but available to assistive technology
    Object.assign(region.style, {
      position: 'absolute',
      width: '1px',
      height: '1px',
      margin: '-1px',
      padding: '0',
      overflow: 'hidden',
      clip: 'rect(0, 0, 0, 0)',
      whiteSpace: 'nowrap',
      border: '0',
    });
    document.body.appendChild(region);
  }

  // Update priority if it changed
  region.setAttribute('aria-live', priority);

  // Clear then set to ensure re-announcement of identical messages
  region.textContent = '';
  requestAnimationFrame(() => {
    region.textContent = message;
  });
}
