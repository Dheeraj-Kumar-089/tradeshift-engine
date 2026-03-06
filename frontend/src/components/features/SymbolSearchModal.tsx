import * as React from "react";
import {
    CommandDialog,
    CommandEmpty,
    CommandGroup,
    CommandInput,
    CommandItem,
    CommandList,
} from "@/components/ui/command";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Button } from "@/components/ui/button";
import { useGame } from "../../hooks/useGame"; // Added to hook into the Game context

export function SymbolSearchModal({
    open,
    onOpenChange
}: {
    open: boolean;
    onOpenChange: (open: boolean) => void;
}) {
    const { setSymbol } = useGame();
    const [activeTab, setActiveTab] = React.useState("All");

    const TABS = ["All", "Indices", "Stocks"];

    const EQUITIES_AND_OPTIONS = [
        { symbol: "NIFTY", name: "Nifty 50 Index", exchange: "NSE", type: "Index", category: "Indices", token: "26000" },
        { symbol: "BANKNIFTY", name: "Nifty Bank Index", exchange: "NSE", type: "Index", category: "Indices", token: "26009" },
        { symbol: "SENSEX", name: "BSE Sensex Index", exchange: "BSE", type: "Index", category: "Indices", token: "1" },
        { symbol: "HDFCBANK", name: "HDFC Bank Ltd", exchange: "NSE", type: "Stock", category: "Stocks", token: "1333" },
        { symbol: "RELIANCE", name: "Reliance Industries Ltd", exchange: "NSE", type: "Stock", category: "Stocks", token: "2885" },
    ];

    const filteredData = activeTab === "All"
        ? EQUITIES_AND_OPTIONS
        : EQUITIES_AND_OPTIONS.filter(item => item.category === activeTab);

    return (
        <CommandDialog open={open} onOpenChange={onOpenChange}>
            <div className="flex flex-col gap-0">
                <div className="px-4 pt-4 pb-2">
                    <h2 className="text-lg font-semibold mb-2">Select Equity / Index</h2>
                    <div className="flex gap-2 overflow-x-auto pb-2 scrollbar-none">
                        {TABS.map(tab => (
                            <Button
                                key={tab}
                                variant="ghost"
                                size="sm"
                                onClick={() => setActiveTab(tab)}
                                className={`h-7 px-3 rounded-full text-xs font-medium transition-all
                            ${activeTab === tab
                                        ? 'bg-primary text-primary-foreground hover:bg-primary/90'
                                        : 'bg-muted text-muted-foreground hover:bg-muted/80'
                                    }
                        `}
                            >
                                {tab}
                            </Button>
                        ))}
                    </div>
                </div>
                <Separator />

                <CommandInput placeholder="Search symbol..." />

                <CommandList className="h-[300px] max-h-[400px]">
                    <CommandEmpty className="py-12 text-center text-sm text-muted-foreground">
                        No results found.
                    </CommandEmpty>

                    <CommandGroup heading={activeTab}>
                        {filteredData.map((item) => (
                            <CommandItem
                                key={`${item.symbol}-${item.exchange}`}
                                value={item.symbol}
                                onSelect={() => {
                                    setSymbol(item.symbol, item.token);
                                    onOpenChange(false);
                                }}
                                className="p-0"
                            >
                                <div
                                    className="flex items-center justify-between w-full px-4 py-3 cursor-pointer hover:bg-muted/50 transition-colors pointer-events-auto"
                                    onMouseDown={(e) => {
                                        e.preventDefault();
                                        e.stopPropagation();
                                        setSymbol(item.symbol, item.token);
                                        onOpenChange(false);
                                    }}
                                    onClick={(e) => {
                                        e.preventDefault();
                                        e.stopPropagation();
                                        setSymbol(item.symbol, item.token);
                                        onOpenChange(false);
                                    }}
                                >
                                    <div className="flex items-center gap-3 overflow-hidden pointer-events-none">
                                        <div className={`w-8 h-8 rounded-full flex items-center justify-center font-bold text-[10px] text-white
                            ${item.category === 'Indices' ? 'bg-blue-600' : 'bg-indigo-600'}
                        `}>
                                            {item.symbol.substring(0, 1)}
                                        </div>
                                        <div className="flex flex-col min-w-0">
                                            <span className="font-bold text-sm truncate">{item.symbol}</span>
                                            <span className="text-xs text-muted-foreground truncate">{item.name}</span>
                                        </div>
                                    </div>

                                    <div className="flex items-center gap-3 shrink-0 pointer-events-none">
                                        <span className="text-xs font-medium text-muted-foreground uppercase">{item.type}</span>
                                        <Badge variant="outline" className="font-medium text-[10px] bg-secondary/50 text-secondary-foreground border-border min-w-[60px] justify-center">
                                            {item.exchange}
                                        </Badge>
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
