export interface CandleData {
    time: number; // Unix timestamp
    open: number;
    high: number;
    low: number;
    close: number;
}

export interface Trade {
    id: string;
    symbol: string;
    type: 'BUY' | 'SELL';
    entryPrice: number;
    quantity: number;
    pnl?: number;
    exitPrice?: number;
    timestamp: Date;
    status: 'OPEN' | 'CLOSED';
}
