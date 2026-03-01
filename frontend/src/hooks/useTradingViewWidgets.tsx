import { useEffect, useRef } from 'react';

function useTradingViewWidget(scriptUrl: string, config: any, height = 600) {
    const containerRef = useRef<HTMLDivElement | null>(null);


    useEffect(

        () => {

            if (!containerRef.current) return;  //if there is no containerRef.current then we will exit.
            if (containerRef.current.dataset.loaded) return; //if the widget is already loaded then we will exit.
            containerRef.current.innerHTML = `<div class="tradingview-widget-container__widget" style="width: 100%; height:${height}px;"></div>`;   // set the innerHTML of the containerRef to a regular div

            const script = document.createElement("script");
            script.src = scriptUrl;
            script.async = true;
            script.innerHTML = JSON.stringify(config);
            containerRef.current.appendChild(script); // append all the properties to the containerRef
            containerRef.current.dataset.loaded = "true"; // set the data-loaded attribute to true

            return () => {
                if (containerRef.current) {
                    containerRef.current.innerHTML = "";
                    delete containerRef.current.dataset.loaded; // delete the data-loaded attribute to true
                }
            };
        },
        [scriptUrl, config, height]  // make sure that it loads whenever the any of the props change such as url, config or height.
    );

    return containerRef;
}

export default useTradingViewWidget;


//depending on the specific chart widget we wan't to load,we then return the containerRef which we just attach to this new chart