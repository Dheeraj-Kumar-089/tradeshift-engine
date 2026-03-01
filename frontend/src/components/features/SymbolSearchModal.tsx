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

export function SymbolSearchModal({
    open,
    onOpenChange
}: {
    open: boolean;
    onOpenChange: (open: boolean) => void;
}) {
    const [activeTab, setActiveTab] = React.useState("All");

    const TABS = ["All", "Stocks", "Forex", "Crypto", "Indices", "Futures", "Bonds", "Economy"];

    const MOCK_DATA = [
        { symbol: "NIFTY", name: "Nifty 50 Index", exchange: "NSE", type: "Index", category: "Indices" },
        { symbol: "BANKNIFTY", name: "Nifty Bank Index", exchange: "NSE", type: "Index", category: "Indices" },
        { symbol: "RELIANCE", name: "Reliance Industries Ltd", exchange: "NSE", type: "Stock", category: "Stocks" },
        { symbol: "TCS", name: "Tata Consultancy Services", exchange: "NSE", type: "Stock", category: "Stocks" },
        { symbol: "HDFCBANK", name: "HDFC Bank Ltd", exchange: "NSE", type: "Stock", category: "Stocks" },
        { symbol: "BTCUSD", name: "Bitcoin / U.S. Dollar", exchange: "BINANCE", type: "Crypto", category: "Crypto" },
        { symbol: "ETHUSD", name: "Ethereum / U.S. Dollar", exchange: "COINBASE", type: "Crypto", category: "Crypto" },
        { symbol: "XAUUSD", name: "Gold Spot / U.S. Dollar", exchange: "OANDA", type: "CFD", category: "Forex" },
        { symbol: "US30", name: "Wall Street 30", exchange: "TVC", type: "CFD", category: "Indices" },
        { symbol: "EURUSD", name: "Euro / U.S. Dollar", exchange: "FXCM", type: "Forex", category: "Forex" },
    ];

    const filteredData = activeTab === "All"
        ? MOCK_DATA
        : MOCK_DATA.filter(item => item.category === activeTab);

    return (
        <CommandDialog open={open} onOpenChange={onOpenChange}>
            <div className="flex flex-col gap-0">
                <div className="px-4 pt-4 pb-2">
                    <h2 className="text-lg font-semibold mb-2">Symbol Search</h2>
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

                <CommandInput placeholder="Search symbol, description, or exchange..." />

                <CommandList className="h-[400px] max-h-[500px]">
                    <CommandEmpty className="py-12 text-center text-sm text-muted-foreground">
                        No results found.
                    </CommandEmpty>

                    <CommandGroup heading={activeTab}>
                        {filteredData.map((item) => (
                            <CommandItem
                                key={`${item.symbol}-${item.exchange}`}
                                onSelect={() => {
                                    // Handle selection
                                    onOpenChange(false);
                                }}
                                className="flex items-center justify-between px-4 py-3 cursor-pointer aria-selected:bg-muted/50"
                            >
                                <div className="flex items-center gap-3 overflow-hidden">
                                    <div className={`w-8 h-8 rounded-full flex items-center justify-center font-bold text-[10px] text-white
                        ${item.category === 'Crypto' ? 'bg-orange-500' :
                                            item.category === 'Forex' ? 'bg-green-600' :
                                                item.category === 'Indices' ? 'bg-blue-600' : 'bg-indigo-600'}
                    `}>
                                        {item.symbol.substring(0, 1)}
                                    </div>
                                    <div className="flex flex-col min-w-0">
                                        <span className="font-bold text-sm truncate">{item.symbol}</span>
                                        <span className="text-xs text-muted-foreground truncate">{item.name}</span>
                                    </div>
                                </div>

                                <div className="flex items-center gap-3 shrink-0">
                                    <span className="text-xs font-medium text-muted-foreground uppercase">{item.type}</span>
                                    <Badge variant="outline" className="font-medium text-[10px] bg-secondary/50 text-secondary-foreground border-border min-w-[60px] justify-center">
                                        {item.exchange}
                                    </Badge>
                                </div>
                            </CommandItem>
                        ))}
                    </CommandGroup>
                </CommandList>
            </div>
        </CommandDialog>
    );
}
