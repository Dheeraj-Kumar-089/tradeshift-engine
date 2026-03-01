import { useState } from 'react';
import { Search, ChevronDown, Plus, LayoutTemplate, Settings, Camera, BarChart2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Separator } from '@/components/ui/separator';
import { SymbolSearchModal } from '../features/SymbolSearchModal';

const TIMEFRAMES = ['1m', '5m', '15m', '1h', '4h', 'D', 'W', 'M'];

const TopToolbar = () => {
    const [isSearchOpen, setIsSearchOpen] = useState(false);

    return (
        <div className="h-12 border-b border-tv-border bg-tv-bg-base flex items-center px-2 gap-1 text-tv-text-secondary select-none">
            <SymbolSearchModal open={isSearchOpen} onOpenChange={setIsSearchOpen} />

            {/* Symbol Search */}
            <Button
                variant="ghost"
                className="h-8 gap-2 px-2 text-tv-text-primary hover:bg-tv-bg-pane/50"
                onClick={() => setIsSearchOpen(true)}
            >
                <div className="w-5 h-5 rounded-full bg-red-500 flex items-center justify-center text-[10px] text-white font-bold">N</div>
                <span className="font-bold">NIFTY 50</span>
                <span className="text-xs bg-tv-bg-pane px-1 rounded text-orange-400">INDEX</span>
                <Plus size={14} />
            </Button>

            <Button variant="ghost" size="icon" className="md:hidden h-8 w-8" onClick={() => setIsSearchOpen(true)}>
                <Search size={18} />
            </Button>

            <Separator orientation="vertical" className="h-6 mx-1 bg-tv-border hidden md:block" />

            {/* Timeframes */}
            <div className="flex items-center">
                {TIMEFRAMES.map((tf) => (
                    <Button
                        key={tf}
                        variant="ghost"
                        className={`h-8 w-8 pk-0 text-sm font-medium ${tf === 'D' ? 'text-tv-primary bg-tv-primary/10' : 'hover:bg-tv-bg-pane/50 hover:text-tv-text-primary'}`}
                    >
                        {tf}
                    </Button>
                ))}
                <Button variant="ghost" className="h-8 w-6 px-0 hover:bg-tv-bg-pane/50">
                    <ChevronDown size={14} />
                </Button>
            </div>

            <Separator orientation="vertical" className="h-6 mx-1 bg-tv-border" />

            {/* Chart Types */}
            <Button variant="ghost" className="h-8 w-10 gap-1 px-1 hover:bg-tv-bg-pane/50">
                <BarChart2 size={18} />
            </Button>

            {/* Indicators */}
            <Button variant="ghost" className="h-8 gap-1 px-2 hover:bg-tv-bg-pane/50 text-tv-text-primary">
                <span className="text-sm">Indicators</span>
            </Button>

            <div className="flex-1" />

            {/* Right Side Controls */}
            <div className="flex items-center gap-1">
                <Button variant="ghost" className="h-8 w-10 px-0 hover:bg-tv-bg-pane/50" title="Chart Layout">
                    <LayoutTemplate size={18} />
                </Button>
                <Button variant="ghost" className="h-8 w-10 px-0 hover:bg-tv-bg-pane/50" title="Settings">
                    <Settings size={18} />
                </Button>
                <Button variant="ghost" className="h-8 w-10 px-0 hover:bg-tv-bg-pane/50" title="Take Snapshot">
                    <Camera size={18} />
                </Button>
            </div>

            <Button className="h-7 bg-blue-600 hover:bg-blue-700 text-white text-xs px-3 ml-2 rounded">
                Publish
            </Button>
        </div>
    );
};

export default TopToolbar;
