import TradingViewWidget from "@/components/ui/TradingViewWidget";
import {
    HEATMAP_WIDGET_CONFIG,
    MARKET_DATA_WIDGET_CONFIG,
    MARKET_OVERVIEW_WIDGET_CONFIG,
    TOP_STORIES_WIDGET_CONFIG
} from "@/lib/constants";

const Home1 = () => {
    const scriptUrl = `https://s3.tradingview.com/external-embedding/embed-widget-`;
    return (
        <div className="flex flex-col h-full w-full overflow-y-auto home-wrapper p-6 bg-tv-bg-base">
            <section className="grid w-full gap-8 home-section">
                <div className="md:col-span-1 xl:col-span-1"> {/* It Wraps our first chart */}
                    <TradingViewWidget title="Market Overview"
                        scriptUrl={`${scriptUrl}market-overview.js`}
                        config={MARKET_OVERVIEW_WIDGET_CONFIG}
                        height={600}
                    />
                </div>
                <div className="md:col-span-1 xl:col-span-2"> {/* It Wraps our second chart */}
                    <TradingViewWidget title="Stock Heatmap"
                        scriptUrl={`${scriptUrl}stock-heatmap.js`}
                        config={HEATMAP_WIDGET_CONFIG}
                        height={600}
                    />
                </div>
            </section>
            <section className="grid w-full gap-8 home-section">
                <div className="h-full md:col-span-1 xl:col-span-1"> {/* It Wraps our first chart */}
                    <TradingViewWidget title="Top Stories"
                        scriptUrl={`${scriptUrl}timeline.js`}
                        config={TOP_STORIES_WIDGET_CONFIG}
                        className="custom-chart"
                        height={600}
                    />
                </div>
                <div className="h-full md:col-span-1 xl:col-span-2"> {/* It Wraps our second chart */}
                    <TradingViewWidget title="Market Quotes"
                        scriptUrl={`${scriptUrl}market-quotes.js`}
                        config={MARKET_DATA_WIDGET_CONFIG}
                        height={600}
                    />
                </div>
            </section>
        </div>
    );
};

export default Home1;