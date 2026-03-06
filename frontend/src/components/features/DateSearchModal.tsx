import * as React from "react";
import {
    CommandDialog,
    CommandEmpty,
    CommandGroup,
    CommandInput,
    CommandItem,
    CommandList,
} from "@/components/ui/command";
import { Separator } from "@/components/ui/separator";
import { useGame } from "../../hooks/useGame";
import { Calendar } from "lucide-react";

export function DateSearchModal({
    open,
    onOpenChange
}: {
    open: boolean;
    onOpenChange: (open: boolean) => void;
}) {
    const { setDate } = useGame();

    const MOCK_DATES = React.useMemo(() => {
        const dates = [];
        for (let i = 0; i < 7; i++) {
            const d = new Date();
            d.setDate(d.getDate() - i);
            const dateStr = d.toISOString().split('T')[0];
            const label = d.toLocaleDateString('en-US', { weekday: 'long', month: 'short', day: 'numeric' });
            dates.push({ date: dateStr, label: i === 0 ? `Today (${label})` : label });
        }
        return dates;
    }, []);

    return (
        <CommandDialog open={open} onOpenChange={onOpenChange}>
            <div className="flex flex-col gap-0">
                <div className="px-4 pt-4 pb-2">
                    <h2 className="text-lg font-semibold mb-2">Select Replay Date</h2>
                    <p className="text-xs text-muted-foreground">Choose a day from the last 7 days to replay</p>
                </div>
                <Separator />
                <CommandInput placeholder="Search date..." />
                <CommandList className="h-[300px] max-h-[400px]">
                    <CommandEmpty className="py-12 text-center text-sm text-muted-foreground">
                        No results found.
                    </CommandEmpty>
                    <CommandGroup heading="Recent Days">
                        {MOCK_DATES.map((item) => (
                            <CommandItem
                                key={item.date}
                                value={item.date}
                                onSelect={() => {
                                    setDate(item.date);
                                    onOpenChange(false);
                                }}
                                className="p-0"
                            >
                                <div
                                    className="flex items-center justify-between w-full px-4 py-3 cursor-pointer hover:bg-muted/50 transition-colors pointer-events-auto"
                                    onMouseDown={(e) => {
                                        e.preventDefault();
                                        e.stopPropagation();
                                        setDate(item.date);
                                        onOpenChange(false);
                                    }}
                                    onClick={(e) => {
                                        e.preventDefault();
                                        e.stopPropagation();
                                        setDate(item.date);
                                        onOpenChange(false);
                                    }}
                                >
                                    <div className="flex items-center gap-3 overflow-hidden pointer-events-none">
                                        <div className="w-8 h-8 rounded-full flex items-center justify-center bg-gray-700 text-white">
                                            <Calendar size={14} />
                                        </div>
                                        <div className="flex flex-col min-w-0">
                                            <span className="font-bold text-sm truncate">{item.label}</span>
                                            <span className="text-xs text-muted-foreground truncate">{item.date}</span>
                                        </div>
                                    </div>
                                </div>
                            </CommandItem>
                        ))}
                    </CommandGroup>
                </CommandList>
            </div>
        </CommandDialog>
    );
}
