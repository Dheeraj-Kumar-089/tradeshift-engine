import React from 'react';
import {
    Crosshair, MousePointer2, Minus, TrendingUp, Paintbrush, Type,
    GitCommit, Heart, Ruler, ZoomIn, Magnet, Lock, EyeOff, Trash2
} from 'lucide-react';
import { Button } from '@/components/ui/button';


const TOOL_GROUPS = [
    [Crosshair, MousePointer2],
    [TrendingUp], // Trend lines
    [Paintbrush], // Gann/Fib
    [Type],       // Text
    [GitCommit],  // Patterns
    [Heart],      // Prediction
    [Ruler],      // Measure
    [ZoomIn],     // Zoom
];

const UTILITY_TOOLS = [Magnet, Lock, EyeOff, Trash2];

const LeftToolbar = () => {

    const ToolButton = ({ icon: Icon, active = false }: { icon: any, active?: boolean; }) => (
        <Button
            variant="ghost"
            className={`w-10 h-10 p-0 rounded-none hover:bg-tv-bg-pane 
        ${active ? 'text-tv-primary bg-tv-bg-pane border-l-2 border-tv-primary' : 'text-tv-text-secondary'}
      `}
        >
            <Icon size={20} strokeWidth={1.5} />
        </Button>
    );

    return (
        <div className="w-[52px] border-r border-tv-border bg-tv-bg-base flex flex-col items-center py-2 h-full select-none">
            {TOOL_GROUPS.map((group, i) => (
                <div key={i} className="flex flex-col w-full items-center">
                    {group.map((Icon, j) => (
                        <ToolButton key={j} icon={Icon} active={i === 0 && j === 0} />
                    ))}
                    {i < TOOL_GROUPS.length - 1 && <div className="h-[1px] w-4 bg-tv-border/50 my-1" />}
                </div>
            ))}

            <div className="mt-auto flex flex-col w-full items-center pb-2">
                {UTILITY_TOOLS.map((Icon, i) => (
                    <ToolButton key={`util-${i}`} icon={Icon} />
                ))}
            </div>
        </div>
    );
};

export default LeftToolbar;
