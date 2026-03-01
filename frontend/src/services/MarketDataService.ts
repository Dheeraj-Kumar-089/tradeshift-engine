

type MessageHandler = (data: any) => void;

export class MarketDataService {
    private ws: WebSocket | null = null;
    private url: string = "ws://localhost:8000/ws/simulation";
    private onMessageCallback: MessageHandler | null = null;

    constructor(url?: string) {
        if (url) this.url = url;
    }

    connect(speed: number) {
        if (this.ws) {
            this.ws.close();
        }

        this.ws = new WebSocket(this.url);

        this.ws.onopen = () => {
            console.log("🟢 Connected to Market Data Service");
            this.sendMessage({ command: "START", speed });
        };

        this.ws.onmessage = (event) => {
            if (this.onMessageCallback) {
                this.onMessageCallback(JSON.parse(event.data));
            }
        };

        this.ws.onclose = () => {
            console.log("🔴 Disconnected from Market Data Service");
        };
    }

    disconnect() {
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
    }

    sendMessage(message: any) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(message));
        }
    }

    onMessage(callback: MessageHandler) {
        this.onMessageCallback = callback;
    }
}

export const marketDataService = new MarketDataService();
