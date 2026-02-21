import { describe, it, expect, beforeEach } from 'vitest';
import { useUIStore } from '../../store/ui';

describe('UIStore', () => {
  beforeEach(() => {
    useUIStore.getState().resetUI();
  });

  it('should start at step 0', () => {
    expect(useUIStore.getState().activeStep).toBe(0);
  });

  it('should set active step', () => {
    useUIStore.getState().setActiveStep(3);
    expect(useUIStore.getState().activeStep).toBe(3);
  });

  it('should set step status and compute progress', () => {
    useUIStore.getState().setStepStatus(0, 'done');
    const state = useUIStore.getState();
    expect(state.stepStatuses[0]).toBe('done');
    expect(state.progress).toBeGreaterThan(0);
  });

  it('should handle running status in progress calculation', () => {
    useUIStore.getState().setStepStatus(0, 'running');
    const state = useUIStore.getState();
    expect(state.stepStatuses[0]).toBe('running');
    expect(state.progress).toBeGreaterThan(0);
  });

  it('should toggle sidebar mode', () => {
    expect(useUIStore.getState().sidebarMode).toBe(true);
    useUIStore.getState().setSidebarMode(false);
    expect(useUIStore.getState().sidebarMode).toBe(false);
  });

  it('should add and remove errors', () => {
    useUIStore.getState().addError({ message: 'Test error', severity: 'error' });
    expect(useUIStore.getState().errors).toHaveLength(1);
    expect(useUIStore.getState().errors[0].message).toBe('Test error');

    const errorId = useUIStore.getState().errors[0].id;
    useUIStore.getState().removeError(errorId);
    expect(useUIStore.getState().errors).toHaveLength(0);
  });

  it('should clear all errors', () => {
    useUIStore.getState().addError({ message: 'Error 1', severity: 'error' });
    useUIStore.getState().addError({ message: 'Error 2', severity: 'warning' });
    expect(useUIStore.getState().errors).toHaveLength(2);

    useUIStore.getState().clearErrors();
    expect(useUIStore.getState().errors).toHaveLength(0);
  });

  it('should add and remove toasts', () => {
    useUIStore.getState().addToast({ message: 'Success!', type: 'success' });
    expect(useUIStore.getState().toasts).toHaveLength(1);

    const toastId = useUIStore.getState().toasts[0].id;
    useUIStore.getState().removeToast(toastId);
    expect(useUIStore.getState().toasts).toHaveLength(0);
  });

  it('should toggle search overlay', () => {
    expect(useUIStore.getState().searchOpen).toBe(false);
    useUIStore.getState().setSearchOpen(true);
    expect(useUIStore.getState().searchOpen).toBe(true);
  });

  it('should toggle settings panel', () => {
    expect(useUIStore.getState().settingsOpen).toBe(false);
    useUIStore.getState().setSettingsOpen(true);
    expect(useUIStore.getState().settingsOpen).toBe(true);
  });

  it('should toggle help panel', () => {
    expect(useUIStore.getState().helpOpen).toBe(false);
    useUIStore.getState().setHelpOpen(true);
    expect(useUIStore.getState().helpOpen).toBe(true);
  });

  it('should set progress directly', () => {
    useUIStore.getState().setProgress(75);
    expect(useUIStore.getState().progress).toBe(75);
  });

  it('should reset all UI state', () => {
    useUIStore.getState().setActiveStep(5);
    useUIStore.getState().setSearchOpen(true);
    useUIStore.getState().addError({ message: 'err', severity: 'error' });

    useUIStore.getState().resetUI();
    const state = useUIStore.getState();
    expect(state.activeStep).toBe(0);
    expect(state.searchOpen).toBe(false);
    expect(state.errors).toHaveLength(0);
  });
});
