import { Play, Pause } from 'lucide-react';
import { useGame } from '../../hooks/useGame';

const PlaybackControls = () => {
  const { isPlaying, togglePlay } = useGame();

  return (
    <div className="absolute bottom-6 left-1/2 -translate-x-1/2 px-6 py-2 rounded-full shadow-2xl flex items-center gap-4 border z-50 transition-all duration-300 hover:shadow-tv-primary/20 hover:-translate-y-1 bg-tv-bg-pane border-tv-border backdrop-blur-md">

      <button
        onClick={togglePlay}
        className={`flex items-center gap-3 transition-all duration-200 group
          ${isPlaying ? 'text-[#f23645]' : 'text-tv-primary'}
        `}
      >
        <div className={`p-2 rounded-full transition-all duration-300 group-hover:scale-110 group-active:scale-90
          ${isPlaying
            ? 'bg-[#f23645] text-white shadow-lg shadow-[#f23645]/30'
            : 'bg-tv-primary text-white shadow-lg shadow-blue-500/30'
          }`}>
          {isPlaying ? <Pause size={20} fill="currentColor" /> : <Play size={20} fill="currentColor" className="ml-0.5" />}
        </div>

        <span className="font-bold text-sm tracking-wide transition-colors text-tv-text-primary">
          {isPlaying ? 'PAUSE' : 'START'} REPLAY
        </span>
      </button>
    </div>
  );
};

export default PlaybackControls;