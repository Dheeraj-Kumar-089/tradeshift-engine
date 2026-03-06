import { useEffect, useRef } from 'react';
import { createChart, ColorType, CrosshairMode } from 'lightweight-charts';
import type { ISeriesApi, UTCTimestamp } from 'lightweight-charts';
import { useGame } from '../../hooks/useGame';
import { useThemeStore } from '../../store/themeStore';

const ChartArea = () => {
  const { currentPrice, currentCandle, isPlaying, historicalCandles, isLoadingHistory, selectedSymbol } = useGame();
  const { theme } = useThemeStore();

  const chartContainerRef = useRef<HTMLDivElement>(null);
  const seriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const chartRef = useRef<any | null>(null);
  const lastTimeRef = useRef<number>(0);

  // ── Initialize Chart ────────────────────────────────────────────────────
  useEffect(() => {
    if (!chartContainerRef.current) return;

    const isDark = theme === 'dark';
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: isDark ? '#D1D4DC' : '#131722',
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          width: 1,
          color: isDark ? '#5c6370' : '#8e96a5',
          style: 0, // Solid line
          visible: true,
          labelVisible: true,
        },
        horzLine: {
          width: 1,
          color: isDark ? '#5c6370' : '#8e96a5',
          style: 0, // Solid line
          visible: true,
          labelVisible: true,
        },
      },
      grid: {
        vertLines: { color: isDark ? '#2A2E39' : '#E0E3EB' },
        horzLines: { color: isDark ? '#2A2E39' : '#E0E3EB' },
      },
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        borderColor: isDark ? '#2A2E39' : '#E0E3EB',
        fixRightEdge: false,
        shiftVisibleRangeOnNewBar: true,
      },
      rightPriceScale: {
        borderColor: isDark ? '#2A2E39' : '#E0E3EB',
      },
    });

    chartRef.current = chart;

    const series = chart.addCandlestickSeries({
      upColor: '#089981',
      downColor: '#f23645',
      borderVisible: false,
      wickUpColor: '#089981',
      wickDownColor: '#f23645',
    });

    seriesRef.current = series;

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: chartContainerRef.current.clientHeight,
        });
      }
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, []);

  // ── Update Theme ────────────────────────────────────────────────────────
  useEffect(() => {
    if (!chartRef.current) return;
    const isDark = theme === 'dark';
    chartRef.current.applyOptions({
      layout: { textColor: isDark ? '#D1D4DC' : '#131722' },
      crosshair: {
        vertLine: { color: isDark ? '#5c6370' : '#8e96a5' },
        horzLine: { color: isDark ? '#5c6370' : '#8e96a5' },
      },
      grid: {
        vertLines: { color: isDark ? '#2A2E39' : '#E0E3EB' },
        horzLines: { color: isDark ? '#2A2E39' : '#E0E3EB' },
      },
      timeScale: { borderColor: isDark ? '#2A2E39' : '#E0E3EB' },
      rightPriceScale: { borderColor: isDark ? '#2A2E39' : '#E0E3EB' },
    });
  }, [theme]);

  // ── Bulk-load Historical Candles whenever they change ───────────────────
  useEffect(() => {
    if (!seriesRef.current || historicalCandles.length === 0) return;

    try {
      const data = historicalCandles.map(c => ({
        time: c.time as UTCTimestamp,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
      }));

      seriesRef.current.setData(data);
      lastTimeRef.current = data[data.length - 1].time as number;

      // Scroll to the right to show the latest candles
      chartRef.current?.timeScale().scrollToRealTime();
      console.log(`📈 Chart loaded ${data.length} historical candles`);
    } catch (err) {
      console.error('Chart setData error:', err);
    }
  }, [historicalCandles]);

  // ── Stream Live Candles from WebSocket ──────────────────────────────────
  useEffect(() => {
    if (!isPlaying || !seriesRef.current || !currentCandle) return;

    try {
      // Detect a timeline reset (e.g. replay loop)
      if (currentCandle.time < lastTimeRef.current) {
        console.log('↺ Timeline Reset — clearing chart');
        seriesRef.current.setData([]);
      }

      const candle = {
        time: currentCandle.time as UTCTimestamp,
        open: currentCandle.open,
        high: currentCandle.high,
        low: currentCandle.low,
        close: currentCandle.close,
      };

      seriesRef.current.update(candle);
      lastTimeRef.current = currentCandle.time;
    } catch (err) {
      console.error('Chart update error:', err);
    }
  }, [currentCandle, isPlaying]);

  return (
    <div className="flex-1 flex flex-col min-h-0 min-w-0 h-full w-full font-sans transition-colors duration-500 bg-tv-bg-base">

      {/* HEADER */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-tv-border transition-colors">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <span className="font-bold text-lg tracking-wide text-tv-text-primary">
              {selectedSymbol || 'NIFTY'}
            </span>
            <span className="bg-tv-primary/10 text-tv-primary text-[10px] font-bold px-1.5 py-0.5 rounded border border-tv-primary/20">
              INDEX
            </span>
            {isLoadingHistory && (
              <span className="text-[10px] text-tv-text-secondary animate-pulse">Loading…</span>
            )}
          </div>
        </div>

        <div
          className={`text-xl font-mono font-bold leading-none ${currentPrice >= (currentCandle?.open ?? currentPrice)
            ? 'text-[#089981]'
            : 'text-[#f23645]'
            }`}
        >
          {currentPrice.toFixed(2)}
        </div>
      </div>

      <div className="flex-1 relative w-full h-full">
        <div ref={chartContainerRef} className="absolute inset-0" />
      </div>
    </div>
  );
};

export default ChartArea;