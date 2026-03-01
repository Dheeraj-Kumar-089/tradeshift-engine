import { useState } from 'react';
import { useGame } from '../../hooks/useGame';

const OrderPanel = () => {
  const { currentPrice, placeOrder, isPlaying, trades, closePosition } = useGame();
  const [qty, setQty] = useState(50);

  return (
    <div className="w-80 border-l border-tv-border bg-tv-bg-pane flex flex-col transition-colors duration-300">

      {/* Header & Inputs */}
      <div className="p-4 border-b border-tv-border">
        <h2 className="font-bold mb-4 flex items-center gap-2 text-tv-text-primary">
          <span className="w-1.5 h-6 bg-tv-primary rounded-full"></span>
          Place Order
        </h2>

        <div className="space-y-4">
          {/* Quantity Selector */}
          <div>
            <label className="text-xs mb-1 block font-medium text-tv-text-secondary">Quantity (Lots)</label>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setQty(Math.max(50, qty - 50))}
                className="w-10 h-10 rounded-lg flex items-center justify-center font-bold transition-all hover:scale-105 active:scale-95 bg-tv-bg-base hover:bg-tv-border text-tv-text-primary"
              >-</button>

              <input
                type="number"
                value={qty}
                readOnly
                className="flex-1 h-10 border border-tv-border rounded-lg text-center font-mono font-bold bg-transparent text-tv-text-primary"
              />

              <button
                onClick={() => setQty(qty + 50)}
                className="w-10 h-10 rounded-lg flex items-center justify-center font-bold transition-all hover:scale-105 active:scale-95 bg-tv-bg-base hover:bg-tv-border text-tv-text-primary"
              >+</button>
            </div>
          </div>

          {/* Action Buttons */}
          <div className="grid grid-cols-2 gap-3">
            <button
              disabled={!isPlaying}
              onClick={() => placeOrder('BUY', qty)}
              className="group relative bg-[#089981] hover:bg-[#067a65] disabled:opacity-50 disabled:cursor-not-allowed text-white py-4 rounded-xl font-bold transition-all duration-200 hover:shadow-lg hover:shadow-[#089981]/25 active:scale-95 active:translate-y-0.5 overflow-hidden"
            >
              <div className="flex flex-col items-center relative z-10">
                <span className="text-[10px] opacity-75 tracking-wider">LONG</span>
                <span className="text-lg">BUY</span>
              </div>
            </button>

            <button
              disabled={!isPlaying}
              onClick={() => placeOrder('SELL', qty)}
              className="group relative bg-[#f23645] hover:bg-[#d12435] disabled:opacity-50 disabled:cursor-not-allowed text-white py-4 rounded-xl font-bold transition-all duration-200 hover:shadow-lg hover:shadow-[#f23645]/25 active:scale-95 active:translate-y-0.5 overflow-hidden"
            >
              <div className="flex flex-col items-center relative z-10">
                <span className="text-[10px] opacity-75 tracking-wider">SHORT</span>
                <span className="text-lg">SELL</span>
              </div>
            </button>
          </div>

          {/* Margin Info */}
          <div className="p-3 rounded-lg text-xs text-center border border-tv-border bg-tv-bg-base text-tv-text-secondary">
            Margin Req: <span className="font-mono font-bold text-tv-text-primary">₹{((currentPrice * qty) / 5).toFixed(0)}</span>
          </div>
        </div>
      </div>

      {/* Positions List */}
      <div className="flex-1 overflow-auto p-4 scrollbar-thin">
        <h3 className="text-xs font-bold uppercase mb-3 text-tv-text-secondary">Active Positions</h3>
        <div className="space-y-3">
          {trades.filter(t => t.status === 'OPEN').map(trade => {
            const multiplier = trade.type === 'BUY' ? 1 : -1;
            const pnl = (currentPrice - trade.entryPrice) * trade.quantity * multiplier;
            const isProfit = pnl >= 0;
            return (
              <div key={trade.id} className="rounded-xl p-3 border border-tv-border bg-tv-bg-base transition-all hover:shadow-md">
                <div className="flex justify-between items-start mb-2">
                  <span className={`text-xs font-bold px-2 py-0.5 rounded shadow-sm ${trade.type === 'BUY' ? 'bg-[#089981]/10 text-[#089981]' : 'bg-[#f23645]/10 text-[#f23645]'}`}>{trade.type}</span>
                  <span className={`font-mono font-bold text-sm ${isProfit ? 'text-[#089981]' : 'text-[#f23645]'}`}>{pnl > 0 ? '+' : ''}{pnl.toFixed(2)}</span>
                </div>
                <div className="flex justify-between text-xs mb-3 text-tv-text-secondary">
                  <span>{trade.quantity} Qty</span>
                  <span>@ {trade.entryPrice.toFixed(2)}</span>
                </div>
                <button onClick={() => closePosition(trade.id)} className="w-full py-1.5 text-xs font-bold rounded-lg transition-colors bg-tv-border hover:bg-gray-600 text-tv-text-primary">Close Position</button>
              </div>
            );
          })}
          {trades.filter(t => t.status === 'OPEN').length === 0 && <div className="text-center text-xs opacity-50 mt-10 text-tv-text-secondary">No active positions</div>}
        </div>
      </div>
    </div>
  );
};

export default OrderPanel;


