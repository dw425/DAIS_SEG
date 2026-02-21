import React from 'react';
import { useUIStore } from './store/ui';
import TopBar from './components/layout/TopBar';
import Sidebar from './components/layout/Sidebar';
import ProgressBar from './components/layout/ProgressBar';
import ErrorPanel from './components/shared/ErrorPanel';
import ToastContainer from './components/shared/Toast';
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

const STEP_LABELS = [
  'Parse', 'Analyze', 'Assess', 'Convert', 'Report',
  'Final Report', 'Validate', 'Value Analysis', 'Summary', 'Admin',
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
];

function TabBar() {
  const activeStep = useUIStore((s) => s.activeStep);
  const setActiveStep = useUIStore((s) => s.setActiveStep);
  const stepStatuses = useUIStore((s) => s.stepStatuses);

  return (
    <div className="shrink-0 border-b border-border bg-gray-900/50 overflow-x-auto">
      <div className="flex gap-0.5 px-2 py-1">
        {STEP_LABELS.map((label, i) => {
          const st = i < 10 ? stepStatuses[i] : 'idle';
          const statusDot =
            st === 'done' ? 'bg-green-400' :
            st === 'running' ? 'bg-primary animate-pulse' :
            st === 'error' ? 'bg-red-400' :
            'bg-gray-600';

          return (
            <button
              key={i}
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

  const ActiveComponent = STEP_COMPONENTS[activeStep] || Step1Parse;

  return (
    <div className="h-screen flex flex-col bg-gray-950 text-gray-100">
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
          <main className="flex-1 overflow-y-auto p-6">
            <div className="max-w-5xl mx-auto">
              <ActiveComponent />
            </div>
          </main>
        </div>
      </div>

      <ProgressBar />
      <ErrorPanel />
      <ToastContainer />
    </div>
  );
}
