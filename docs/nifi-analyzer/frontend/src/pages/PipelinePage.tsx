import React from 'react';
import App from '../App';

/**
 * PipelinePage wraps the existing App component (step-based pipeline view).
 * This preserves all existing functionality within the new router.
 */
export default function PipelinePage() {
  return <App />;
}
