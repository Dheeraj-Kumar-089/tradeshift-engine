import React, { useState, useEffect, createContext } from 'react';
import type { CandleData, Trade } from '../types';
import { marketDataService } from '../services/MarketDataService';

interface GameState {
  isPlaying: boolean;
  speed: number;
  balance: number;
  currentPrice: number;
  currentCandle: CandleData | null;
  trades: Trade[];
  togglePlay: () => void;
  setSpeed: (s: number) => void;
  placeOrder: (type: 'BUY' | 'SELL', qty: number) => void;
  closePosition: (tradeId: string) => void;
  resetSimulation: () => void;
}


export const GameContext = createContext<GameState | null>(null);

export const GameProvider: React.FC<{ children: React.ReactNode; }> = ({ children }) => {
  const [isPlaying, setIsPlaying] = useState(false);
  const [speed, setSpeed] = useState(1);
  const [balance, setBalance] = useState(100000);
  const [currentPrice, setCurrentPrice] = useState(21500);
  const [currentCandle, setCurrentCandle] = useState<CandleData | null>(null);
  const [trades, setTrades] = useState<Trade[]>([]);

  useEffect(() => {
    if (!isPlaying) {
      marketDataService.disconnect();
      return;
    }

    marketDataService.connect(speed);

    marketDataService.onMessage((payload: any) => {
      if (payload.type === 'CANDLE') {
        const d = payload.data;
        const rawTime = new Date(d.timestamp).getTime() / 1000;
        const timestamp = rawTime + 19800;

        const newCandle = {
          time: timestamp,
          open: d.open,
          high: d.high,
          low: d.low,
          close: d.close
        };

        setCurrentCandle(newCandle);
        setCurrentPrice(d.close);
      }

      if (payload.type === 'BATCH') {
        const batchData = payload.data;
        if (batchData && batchData.length > 0) {
          const lastItem = batchData[batchData.length - 1];
          setCurrentPrice(lastItem.price);

          setCurrentCandle(prevCandle => {
            let newCandle = prevCandle ? { ...prevCandle } : null;

            batchData.forEach((tick: any) => {
              const rawTime = new Date(tick.timestamp).getTime() / 1000;
              const shiftedTime = rawTime + 19800;
              const candleTime = Math.floor(shiftedTime / 60) * 60;

              if (!newCandle || candleTime !== newCandle.time) {
                newCandle = {
                  time: candleTime,
                  open: tick.price,
                  high: tick.price,
                  low: tick.price,
                  close: tick.price
                };
              } else {
                newCandle.high = Math.max(newCandle.high, tick.price);
                newCandle.low = Math.min(newCandle.low, tick.price);
                newCandle.close = tick.price;
              }
            });
            return newCandle;
          });
        }
      }

      if (payload.type === 'TICK') {
        setCurrentPrice(payload.data.price);
      }
    });

    return () => {
      marketDataService.disconnect();
    };
  }, [isPlaying, speed]);

  const togglePlay = () => setIsPlaying(!isPlaying);

  const placeOrder = (type: 'BUY' | 'SELL', quantity: number) => {
    const newTrade: Trade = {
      id: Math.random().toString(36).substr(2, 9),
      symbol: "NIFTY 50",
      type,
      entryPrice: currentPrice,
      quantity,
      timestamp: new Date(currentCandle ? currentCandle.time * 1000 : Date.now()),
      status: 'OPEN'
    };
    setTrades([newTrade, ...trades]);
  };

  const closePosition = (tradeId: string) => {
    setTrades(prevTrades => prevTrades.map(trade => {
      if (trade.id === tradeId && trade.status === 'OPEN') {
        const exitPrice = currentPrice;
        const multiplier = trade.type === 'BUY' ? 1 : -1;
        const pnl = (exitPrice - trade.entryPrice) * trade.quantity * multiplier;
        setBalance(prev => prev + pnl);
        return { ...trade, status: 'CLOSED', exitPrice, pnl };
      }
      return trade;
    }));
  };

  const resetSimulation = () => {
    setIsPlaying(false);
    setBalance(100000);
    setTrades([]);
    setCurrentCandle(null);
  };

  return (
    <GameContext.Provider value={{
      isPlaying, speed, balance, currentPrice, currentCandle, trades,
      togglePlay, setSpeed, placeOrder, closePosition, resetSimulation
    }}>
      {children}
    </GameContext.Provider>
  );
};