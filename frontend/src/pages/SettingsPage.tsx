import { Activity, AlertCircle, Moon, Sun, Monitor } from 'lucide-react';
import { useGame } from '../hooks/useGame';
import { useTheme } from '../context/ThemeContext';
import { Button } from '@/components/ui/button';

const SettingsPage = () => {
  const { speed, setSpeed, resetSimulation } = useGame();
  const { theme, setTheme } = useTheme();

  return (
    <div className="p-8 w-full max-w-2xl mx-auto font-sans">
      <h2 className="text-2xl font-bold text-tv-text-primary mb-6">Settings</h2>

      <div className="space-y-6">
        {/* Theme Settings */}
        <div className="bg-tv-bg-pane p-6 rounded-lg border border-tv-border">
          <div className="flex items-center gap-3 mb-4 text-tv-text-primary">
            <Sun className="text-tv-primary" />
            <h3 className="font-semibold">Appearance</h3>
          </div>
          <div className="flex gap-2">
            <Button
              variant={theme === 'light' ? 'default' : 'outline'}
              onClick={() => setTheme('light')}
              className="flex-1 gap-2"
            >
              <Sun size={16} /> Light
            </Button>
            <Button
              variant={theme === 'dark' ? 'default' : 'outline'}
              onClick={() => setTheme('dark')}
              className="flex-1 gap-2"
            >
              <Moon size={16} /> Dark
            </Button>
            <Button
              variant={theme === 'system' ? 'default' : 'outline'}
              onClick={() => setTheme('system')}
              className="flex-1 gap-2"
            >
              <Monitor size={16} /> System
            </Button>
          </div>
        </div>

        {/* Speed Control Section */}
        <div className="bg-tv-bg-pane p-6 rounded-lg border border-tv-border">
          <div className="flex items-center gap-3 mb-4 text-tv-text-primary">
            <Activity className="text-tv-primary" />
            <h3 className="font-semibold">Simulation Speed</h3>
          </div>
          <p className="text-tv-text-secondary text-sm mb-4">Control how fast the historical data is replayed.</p>

          <div className="flex items-center gap-4">
            <input
              type="range"
              min="1"
              max="20"
              value={speed}
              onChange={(e) => setSpeed(parseInt(e.target.value))}
              className="flex-1 h-2 bg-tv-bg-base rounded-lg appearance-none cursor-pointer accent-tv-primary"
            />
            <span className="text-tv-text-primary font-mono w-12 text-right">{speed}x</span>
          </div>
        </div>

        {/* Reset Section */}
        <div className="bg-tv-bg-pane p-6 rounded-lg border border-tv-border">
          <div className="flex items-center gap-3 mb-4 text-tv-text-primary">
            <AlertCircle className="text-[#f23645]" />
            <h3 className="font-semibold">Danger Zone</h3>
          </div>
          <div className="flex items-center justify-between">
            <p className="text-tv-text-secondary text-sm">Reset account balance and clear trade history.</p>
            <button
              onClick={resetSimulation}
              className="px-4 py-2 bg-[#f23645]/10 text-[#f23645] hover:bg-[#f23645]/20 border border-[#f23645]/20 rounded transition-colors text-sm font-medium"
            >
              Reset Simulation
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SettingsPage;