// TradingViewWidget.jsx
import useTradingViewWidget from '@/hooks/useTradingViewWidgets';
import { cn } from '@/lib/utils';


interface TradingViewWidgetProps {  //defining the props that will be passed to the TradingViewWidget component
    title: string;
    scriptUrl: string;
    config: any;
    height?: number;
    className?: string;
}

function TradingViewWidget({ title, scriptUrl, config, height = 600, className }: TradingViewWidgetProps) {
    const containerRef = useTradingViewWidget(scriptUrl, config, height); // the props to pass here will also be coming as props to the tradingviewwidget component
    // now pass this containerRef to the new div that'll actually render our widget.

    return (
        // we can furthur style this new reusable trading view widget component so that we can actually display all kinds of widgets, not just charts 
        <div className='w-full'>
            {title && <h3 className="font-semibold text-2xl text-gray-100 mb-5">{title}</h3>}
            {/* we can also pass our own styles to it by wrapping everything in a cn which stands for classNames . so we now always renders this class name but if we want to, we can also pass some additional classNames that are coming through props */}
            <div className={cn('tradingview-widget-container', className)} ref={containerRef}>
                <div className="tradingview-widget-container__widget" style={{ height, width: "100%" }} />
            </div>
        </div>
    );
}

export default TradingViewWidget;
