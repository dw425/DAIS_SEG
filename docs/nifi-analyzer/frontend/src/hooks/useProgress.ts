import { useMemo } from 'react';
import { useUIStore, type StepStatus } from '../store/ui';

export function useProgress() {
  const stepStatuses = useUIStore((s) => s.stepStatuses);

  return useMemo(() => {
    const total = 8; // 8 pipeline steps (indices 0-7)
    let completed = 0;
    let running = 0;
    let errored = 0;

    for (let i = 0; i < total; i++) {
      const st: StepStatus = stepStatuses[i];
      if (st === 'done') completed++;
      else if (st === 'running') running++;
      else if (st === 'error') errored++;
    }

    const percent = Math.min(100, Math.round(((completed + running * 0.5) / total) * 100));

    return { completed, running, errored, total, percent };
  }, [stepStatuses]);
}
