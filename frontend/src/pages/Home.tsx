import { useState } from 'react';
import { Activity, ChevronLeft, ChevronRight, Wallet } from 'lucide-react';
import Sidebar from '../components/layout/Sidebar';
import type { Page } from '../components/layout/Sidebar';
import ChartArea from '../components/features/ChartArea';
import OrderPanel from '../components/features/OrderPanel';
import PlaybackControls from '../components/features/PlaybackControls';
import HistoryPage from './HistoryPage';
import SettingsPage from './SettingsPage';
import { useGame } from '../hooks/useGame';
import TopToolbar from '../components/layout/TopToolbar';
import LeftToolbar from '../components/layout/LeftToolbar';
import NewsPanel from '../components/features/NewsPanel';
import { Button } from '@/components/ui/button';

const Home = () => {
  const [page, setPage] = useState<Page>('terminal');
  const [isRightPanelOpen, setIsRightPanelOpen] = useState(false);
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const { speed, setSpeed } = useGame();

  return (
    <div className="flex h-full w-full bg-background text-foreground overflow-hidden font-geist">
      {/* Sidebar Toggle Button (Visible when sidebar is closed) */}
      <div className={`absolute left-0 top-1/2 -translate-y-1/2 z-50 transition-all duration-300 ${isSidebarOpen ? 'translate-x-20' : 'translate-x-0'}`}>
        <Button
          variant="ghost"
          size="icon"
          className="h-12 w-4 rounded-r-md rounded-l-none bg-sidebar border-y border-r border-sidebar-border shadow-md hover:bg-sidebar-accent text-sidebar-foreground"
          onClick={() => setIsSidebarOpen(!isSidebarOpen)}
        >
          {isSidebarOpen ? <ChevronLeft size={14} /> : <ChevronRight size={14} />}
        </Button>
      </div>

      {/* Main Sidebar (Collapsible) */}
      <div className={`transition-all duration-300 ease-in-out overflow-hidden border-r border-sidebar-border bg-sidebar
          ${isSidebarOpen ? 'w-20 opacity-100' : 'w-0 opacity-0 border-none'}
       `}>
        <Sidebar page={page} setPage={setPage} />
      </div>

      <div className="flex-1 flex flex-col min-w-0">
        <main className="flex-1 relative flex overflow-hidden">
          {page === 'terminal' && (
            <div className="flex h-full w-full">
              {/* Left Toolbar (Drawing Tools - Static) */}
              <LeftToolbar />

              {/* Center Area (Chart + Top Toolbar) */}
              <div className="flex-1 flex flex-col relative min-w-0">
                <TopToolbar />

                <div className="flex-1 relative bg-card overflow-hidden">
                  <ChartArea />

                  {/* Playback Controls (Bottom Center) */}
                  <PlaybackControls />

                  {/* Speed Control (Bottom Left) */}
                  <div className="absolute bottom-4 left-4 z-40">
                    <div className="p-2 rounded-lg flex items-center gap-3 shadow-lg backdrop-blur-md border border-border/50 bg-background/80 transition-all duration-300 hover:scale-105">
                      <div className="p-1.5 rounded-md bg-primary/10 text-primary">
                        <Activity size={14} />
                      </div>
                      <div className="flex flex-col gap-0.5">
                        <span className="text-[9px] font-semibold uppercase tracking-wider leading-none text-muted-foreground">Speed</span>
                        <div className="flex items-center gap-2">
                          <input
                            type="range"
                            min="1"
                            max="20"
                            step="1"
                            value={speed}
                            onChange={(e) => setSpeed(Number(e.target.value))}
                            className="w-16 h-1 bg-secondary rounded-lg appearance-none cursor-pointer accent-primary"
                          />
                          <span className="text-[10px] font-mono min-w-[20px] text-right font-bold text-primary">{speed}x</span>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Margin Display (Bottom Right) */}
                  <div className="absolute bottom-4 right-4 z-40">
                    <div className="px-3 py-1.5 rounded-md bg-emerald-500/10 border border-emerald-500/20 flex items-center gap-2 text-emerald-500 backdrop-blur-md">
                      <Wallet size={14} />
                      <span className="text-xs font-bold">₹1,00,000</span>
                    </div>
                  </div>
                </div>
              </div>

              {/* Toggle Button for Right Sidebar */}
              <Button
                variant="ghost"
                size="icon"
                className="absolute right-0 top-1/2 -translate-y-1/2 z-50 h-12 w-4 rounded-l-md rounded-r-none bg-tv-bg-base border-y border-l border-tv-border shadow-md hover:bg-tv-bg-pane text-tv-text-secondary"
                style={{ right: isRightPanelOpen ? '320px' : '0' }}
                onClick={() => setIsRightPanelOpen(!isRightPanelOpen)}
              >
                {isRightPanelOpen ? <ChevronRight size={14} /> : <ChevronLeft size={14} />}
              </Button>

              {/* Right Sidebar (Order Panel / News) */}
              <div className={`flex bg-tv-bg-base border-l border-tv-border transition-all duration-300 ease-in-out overflow-hidden
                  ${isRightPanelOpen ? 'w-80 opacity-100' : 'w-0 opacity-0'}
               `}>
                <div className="w-80 flex flex-col h-full">
                  {/* Tabs or Sections could go here */}
                  <div className="flex-1 overflow-y-auto custom-scrollbar">
                    <div className="p-0">
                      <OrderPanel />
                    </div>

                    {/* News Section below Order Panel */}
                    <div className="h-[250px] border-t border-tv-border">
                      <NewsPanel />
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {page === 'history' && (
            <div className="flex-1 overflow-auto bg-background p-6">
              <div className="max-w-7xl mx-auto">
                <h1 className="text-3xl font-bold mb-6 text-primary">Trade History</h1>
                <div className="rounded-xl border border-border bg-card shadow-sm overflow-hidden">
                  <HistoryPage />
                </div>
              </div>
            </div>
          )}

          {page === 'settings' && (
            <div className="flex-1 overflow-auto bg-background p-6">
              <div className="max-w-4xl mx-auto">
                <SettingsPage />
              </div>
            </div>
          )}
        </main>
      </div>
    </div>
  );
};

export default Home;