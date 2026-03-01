import { success } from "zod";
import {inngest} from "./client";
import {PERSONALIZED_WELCOME_EMAIL_PROMPT} from "./prompts";


export const sendSignUpEmail = inngest.createFunction(
    {id:"send-signup-email"},
    {event: "user/signup"},
    async ({event,step}) => {
        const userProfile = `
        -Country: ${event.data.country}
        -Investement goals: ${event.data.investment_goals}
        -Risk tolerance: ${event.data.risk_tolerance}
        -Preferred industries: ${event.data.preferred_industries}
        `

        const prompt = PERSONALIZED_WELCOME_EMAIL_PROMPT.replace("{{userProfile}}", userProfile);  // send the necessary info to personalized email prompt
        const response = await step.ai.infer('generate-welcome-intro', {
            model: step.ai.models.gemini({model:'gemini-2.5-flash-lite-preview-06-17'}),
            body: {
                contents:[
                    {
                        role:'user',
                        parts:[
                            {text:prompt}
                        ]                        
                    }]
                }
            })


        await step.run('send-welcome-email',async()=>{
            const part = response.candidates?.[0]?.content?.parts?.[0];  // access the first part of the response
            const introText = (part && 'text' in part ? part.text : null) || 'Thanks for joining Tradeshift. You now have the tools to track markets and make smarter moves.' // if part exits and if word text found in that part

            //Email sending logic
        })

        return {
            success:true,
            message:'Welcome email sent successfully'
        }
    }
);