import React from 'react';
import { useUIStore } from './store/ui';
import TopBar from './components/layout/TopBar';
import Sidebar from './components/layout/Sidebar';
import ProgressBar from './components/layout/ProgressBar';
import ErrorPanel from './components/shared/ErrorPanel';
import ErrorModal from './components/shared/ErrorModal';
import PipelineProgress from './components/shared/PipelineProgress';
import ToastContainer from './components/shared/Toast';
import SearchOverlay from './components/shared/SearchOverlay';
import SettingsPanel from './components/shared/SettingsPanel';
import HelpPanel from './components/shared/HelpPanel';
import SkipLink from './components/shared/SkipLink';
import Step1Parse from './components/steps/Step1Parse';
import Step2Analyze from './components/steps/Step2Analyze';
import Step3Assess from './components/steps/Step3Assess';
import Step4Convert from './components/steps/Step4Convert';
import Step5Report from './components/steps/Step5Report';
import Step6FinalReport from './components/steps/Step6FinalReport';
import Step7Validate from './components/steps/Step7Validate';
import Step8ValueAnalysis from './components/steps/Step8ValueAnalysis';
import SummaryPage from './components/steps/SummaryPage';
import AdminConsole from './components/admin/AdminConsole';
import PortfolioDashboard from './components/dashboard/PortfolioDashboard';
import RunHistory from './components/history/RunHistory';
import ComparisonDashboard from './components/compare/ComparisonDashboard';
import { useKeyboardShortcuts } from './hooks/useKeyboardShortcuts';
import { useSessionPersistence } from './hooks/useSessionPersistence';
import { useTranslation } from './hooks/useTranslation';

const STEP_KEYS = [
  'nav.parse', 'nav.analyze', 'nav.assess', 'nav.convert', 'nav.report',
  'nav.finalReport', 'nav.validate', 'nav.valueAnalysis', 'nav.summary', 'nav.admin',
  'nav.dashboard', 'nav.history', 'nav.compare',
];

const STEP_LABELS_FALLBACK = [
  'Parse', 'Analyze', 'Assess', 'Convert', 'Report',
  'Final Report', 'Validate', 'Value Analysis', 'Summary', 'Admin',
  'Dashboard', 'History', 'Compare',
];

const STEP_COMPONENTS: React.FC[] = [
  Step1Parse,
  Step2Analyze,
  Step3Assess,
  Step4Convert,
  Step5Report,
  Step6FinalReport,
  Step7Validate,
  Step8ValueAnalysis,
  SummaryPage,
  AdminConsole,
  PortfolioDashboard,
  RunHistory,
  ComparisonDashboard,
];

function TabBar() {
  const activeStep = useUIStore((s) => s.activeStep);
  const setActiveStep = useUIStore((s) => s.setActiveStep);
  const stepStatuses = useUIStore((s) => s.stepStatuses);
  const { t } = useTranslation();

  return (
    <div className="shrink-0 border-b border-border bg-gray-900/50 dark:bg-gray-900/50 overflow-x-auto" role="tablist" aria-label="Pipeline steps">
      <div className="flex gap-0.5 px-2 py-1">
        {STEP_KEYS.map((key, i) => {
          const label = t(key, STEP_LABELS_FALLBACK[i]);
          const st = i < 10 ? stepStatuses[i] : 'idle';
          const statusDot =
            st === 'done' ? 'bg-green-400' :
            st === 'running' ? 'bg-primary animate-pulse' :
            st === 'error' ? 'bg-red-400' :
            'bg-gray-600';

          return (
            <button
              key={i}
              role="tab"
              aria-selected={i === activeStep}
              aria-label={`${label} step${st !== 'idle' ? `, status: ${st}` : ''}`}
              onClick={() => setActiveStep(i)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-t-lg text-xs font-medium transition shrink-0
                ${i === activeStep ? 'bg-gray-800 text-gray-100 border-b-2 border-primary' : 'text-gray-500 hover:text-gray-300 hover:bg-gray-800/50'}`}
            >
              <span className={`w-1.5 h-1.5 rounded-full ${statusDot}`} />
              {i < 8 ? `${i + 1}. ` : ''}{label}
            </button>
          );
        })}
      </div>
    </div>
  );
}

export default function App() {
  const activeStep = useUIStore((s) => s.activeStep);
  const sidebarMode = useUIStore((s) => s.sidebarMode);
  const pipelineRunning = useUIStore((s) => s.pipelineRunning);

  // Register global keyboard shortcuts
  useKeyboardShortcuts();

  // Persist pipeline state to localStorage
  useSessionPersistence();

  const ActiveComponent = STEP_COMPONENTS[activeStep] || Step1Parse;

  // Show pipeline progress view when pipeline is running (stays on this screen until complete)
  const showProgress = pipelineRunning || useUIStore.getState().stepStatuses.some((s) => s === 'running');

  return (
    <div className="h-screen flex flex-col bg-gray-950 dark:bg-gray-950 text-gray-100 dark:text-gray-100">
      <SkipLink />
      <TopBar />

      <div className="flex-1 flex overflow-hidden">
        {/* Sidebar or Tabs */}
        {sidebarMode ? (
          <Sidebar />
        ) : null}

        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Tab bar when sidebar is off */}
          {!sidebarMode && <TabBar />}

          {/* Main content */}
          <main id="main-content" className="flex-1 overflow-y-auto p-6" role="main" tabIndex={-1}>
            <div className="max-w-5xl mx-auto">
              {showProgress ? <PipelineProgress /> : <ActiveComponent />}
            </div>
          </main>
        </div>
      </div>

      <ProgressBar />
      <ErrorPanel />
      <ErrorModal />
      <ToastContainer />

      {/* Overlays / Drawers */}
      <SearchOverlay />
      <SettingsPanel />
      <HelpPanel />
    </div>
  );
}
