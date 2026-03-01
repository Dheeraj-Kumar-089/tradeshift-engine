import { Link } from 'react-router-dom';
import { Wallet, Sun, Moon } from 'lucide-react';
import { useGame } from '../../hooks/useGame';
import { useThemeStore } from '../../store/themeStore';
import { NavItems } from './NavItems';
import { UserDropdown } from './UserDropdown';

const Topbar = () => {
  const { balance, isPlaying } = useGame();
  const { theme, toggleTheme } = useThemeStore();

  return (
    <header className="sticky top-0 h-14 min-h-[56px] border-b border-tv-border bg-tv-bg-pane flex items-center justify-between px-6 transition-colors duration-300 z-50">

      <div className="flex items-center gap-4">
        <Link to="/Home1">
          <img src="/assets/icons/logo.svg" alt="Logo" width={140} height={32} className='h-8 w-auto cursor-pointer' />
        </Link>
        <h1 className="text-xl font-bold tracking-wide text-tv-text-primary">
          QUANT<span className="text-tv-primary">SIM</span>
        </h1>
        <span className={`px-2 py-0.5 rounded text-xs font-mono border transition-colors duration-300
          ${isPlaying
            ? 'bg-green-500/10 text-green-500 border-green-500/20'
            : 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20'
          }`}>
          {isPlaying ? 'LIVE FEED' : 'PAUSED'}
        </span>
      </div>

      <NavItems />

      <div className="flex items-center gap-4">
        {/* THEME TOGGLE BUTTON */}
        <button
          onClick={toggleTheme}
          className="p-2 rounded-full text-tv-text-secondary hover:text-tv-text-primary hover:bg-tv-text-primary/10 transition-colors duration-300"
          title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
        >
          {theme === 'dark' ? <Sun size={20} /> : <Moon size={20} />}
        </button>

        <div className="h-6 w-[1px] bg-tv-border"></div>

        <div className="flex items-center gap-3">
          <div className="flex flex-col items-end">
            <span className="text-[10px] uppercase font-bold tracking-wider text-tv-text-secondary">Account Equity</span>
            <span className="font-mono text-lg font-bold text-tv-text-primary">₹{balance.toLocaleString('en-IN', { maximumFractionDigits: 2 })}</span>
          </div>
          <div className="w-10 h-10 rounded-full flex items-center justify-center bg-tv-primary text-white shadow-lg shadow-blue-500/20">
            <Wallet size={20} />
          </div>
        </div>

        <UserDropdown />
      </div>
    </header>
  );
};

export default Topbar;