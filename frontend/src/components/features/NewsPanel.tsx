import React from 'react';
import { Newspaper } from 'lucide-react';

const NewsPanel = () => {
    const newsItems = [
        { title: "Nifty hits all-time high amid global rally", time: "2m ago", sentiment: "positive" },
        { title: "Tech stocks face pressure as yields rise", time: "15m ago", sentiment: "negative" },
        { title: "RBI expected to hold rates steady in next meeting", time: "1h ago", sentiment: "neutral" },
        { title: "Q3 Earnings: Reliance beats estimates", time: "2h ago", sentiment: "positive" },
    ];

    return (
        <div className="flex flex-col h-full bg-tv-bg-base border-l border-tv-border">
            <div className="h-12 border-b border-tv-border flex items-center px-4 font-bold text-sm text-tv-text-primary gap-2">
                <Newspaper size={16} />
                <span>News AI</span>
            </div>
            <div className="flex-1 overflow-y-auto p-2 space-y-2">
                {newsItems.map((item, i) => (
                    <div key={i} className="p-3 rounded bg-tv-bg-pane border border-tv-border hover:border-tv-primary/50 cursor-pointer transition-colors group">
                        <h4 className="text-xs font-medium text-tv-text-primary mb-1 group-hover:text-tv-primary transition-colors">{item.title}</h4>
                        <div className="flex justify-between items-center mt-2">
                            <span className="text-[10px] text-tv-text-secondary">{item.time}</span>
                            <span className={`text-[9px] px-1.5 py-0.5 rounded uppercase font-bold
                                ${item.sentiment === 'positive' ? 'bg-emerald-500/10 text-emerald-500' :
                                    item.sentiment === 'negative' ? 'bg-rose-500/10 text-rose-500' : 'bg-yellow-500/10 text-yellow-500'}
                            `}>
                                {item.sentiment}
                            </span>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
};

export default NewsPanel;
