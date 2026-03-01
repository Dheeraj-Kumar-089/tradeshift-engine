import { useGame } from '../hooks/useGame';

const HistoryPage = () => {
  const { trades } = useGame();

  return (
    <div className="p-8 w-full max-w-4xl mx-auto font-sans">
      <h2 className="text-2xl font-bold text-tv-text-primary mb-6">Trade History</h2>

      <div className="bg-tv-bg-pane rounded-lg border border-tv-border overflow-hidden">
        <table className="w-full text-left text-sm text-tv-text-secondary">
          <thead className="bg-tv-bg-base text-tv-text-secondary uppercase text-xs">
            <tr>
              <th className="px-6 py-3">Time</th>
              <th className="px-6 py-3">Symbol</th>
              <th className="px-6 py-3">Type</th>
              <th className="px-6 py-3 text-right">Qty</th>
              <th className="px-6 py-3 text-right">Entry</th>
              <th className="px-6 py-3 text-right">Exit</th>
              <th className="px-6 py-3 text-right">P&L</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-tv-border">
            {/* Filter and Map: Only show CLOSED trades in the table */}
            {trades.filter(t => t.status === 'CLOSED').map(trade => (
              <tr key={trade.id} className="hover:bg-tv-bg-base/50 transition-colors">
                <td className="px-6 py-4">{trade.timestamp.toLocaleTimeString()}</td>
                <td className="px-6 py-4 text-tv-text-primary font-medium">{trade.symbol}</td>
                <td className="px-6 py-4">
                  <span className={`${trade.type === 'BUY' ? 'text-[#089981]' : 'text-[#f23645]'}`}>
                    {trade.type}
                  </span>
                </td>
                <td className="px-6 py-4 text-right">{trade.quantity}</td>
                <td className="px-6 py-4 text-right">{trade.entryPrice.toFixed(2)}</td>
                <td className="px-6 py-4 text-right">{trade.exitPrice?.toFixed(2)}</td>
                <td className={`px-6 py-4 text-right font-mono font-bold ${(trade.pnl || 0) >= 0 ? 'text-[#089981]' : 'text-[#f23645]'}`}>
                  {(trade.pnl || 0) > 0 ? '+' : ''}{trade.pnl?.toFixed(2)}
                </td>
              </tr>
            ))}

            {/* Empty State Message */}
            {trades.filter(t => t.status === 'CLOSED').length === 0 && (
              <tr>
                <td colSpan={7} className="px-6 py-8 text-center text-tv-text-secondary">No closed trades yet.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default HistoryPage;