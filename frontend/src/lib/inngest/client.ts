import { Inngest } from "inngest";


// Client-side Inngest instance for sending events only
const apiKey = import.meta.env.VITE_GEMINI_API_KEY;

export const inngest = new Inngest({
    id: "tradeshift",
    ai: { gemini: { apiKey: apiKey! } },  //choose which ai model you want to use
});