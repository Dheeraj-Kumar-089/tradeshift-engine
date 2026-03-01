import warnings
# Suppress warnings from google.generativeai and inngest
warnings.filterwarnings("ignore", category=FutureWarning)

import inngest
import os
import google.generativeai as genai
from .client import inngest_client
from fastapi_mail import FastMail, MessageSchema, MessageType
from app.config import conf

# Configure Gemini
# Ensure GEMINI_API_KEY is allowed to be read from os.environ
# If not set, it might fail.
api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    genai.configure(api_key=api_key)

PERSONALIZED_WELCOME_EMAIL_PROMPT = """Generate highly personalized HTML content that will be inserted into an email template at the {{intro}} placeholder.

User profile data:
{{userProfile}}

PERSONALIZATION REQUIREMENTS:
You MUST create content that is obviously tailored to THIS specific user by:

IMPORTANT: Do NOT start the personalized content with "Welcome" since the email header already says "Welcome aboard {{name}}". Use alternative openings like "Thanks for joining", "Great to have you", "You're all set", "Perfect timing", etc.

1. **Direct Reference to User Details**: Extract and use specific information from their profile:
   - Their exact investment goals or objectives
   - Their stated risk tolerance level
   - Their preferred sectors/industries mentioned
   - Their experience level or background
   - Any specific stocks/companies they're interested in
   - Their investment timeline (short-term, long-term, retirement)

2. **Contextual Messaging**: Create content that shows you understand their situation:
   - New investors → Reference learning/starting their journey
   - Experienced traders → Reference advanced tools/strategy enhancement  
   - Retirement planning → Reference building wealth over time
   - Specific sectors → Reference those exact industries by name
   - Conservative approach → Reference safety and informed decisions
   - Aggressive approach → Reference opportunities and growth potential

3. **Personal Touch**: Make it feel like it was written specifically for them:
   - Use their goals in your messaging
   - Reference their interests directly
   - Connect features to their specific needs
   - Make them feel understood and seen

CRITICAL FORMATTING REQUIREMENTS:
- Return ONLY clean HTML content with NO markdown, NO code blocks, NO backticks
- Use SINGLE paragraph only: <p class="mobile-text" style="margin: 0 0 30px 0; font-size: 16px; line-height: 1.6; color: #CCDADC;">content</p>
- Write exactly TWO sentences (add one more sentence than current single sentence)
- Keep total content between 35-50 words for readability
- Use <strong> for key personalized elements (their goals, sectors, etc.)
- DO NOT include "Here's what you can do right now:" as this is already in the template
- Make every word count toward personalization
- Second sentence should add helpful context or reinforce the personalization
"""

@inngest_client.create_function(
    fn_id="send-signup-email",
    trigger=inngest.TriggerEvent(event="app/user.created"),
)
async def send_signup_email(ctx: inngest.Context, step: inngest.Step):
    try:
        print(f"👉 [STEP START] send_signup_email triggered for {ctx.event.data.get('email')}")
        event = ctx.event
        email_address = event.data.get('email')
        first_name = event.data.get('firstName', 'Trader')
        
        # helper for AI generation
        async def generate_intro():
            print("  👉 [AI] generate_intro started")
            if not api_key:
                print("  ⚠️ [AI] No API Key, returning default")
                return f"Welcome to Tradeshift, {first_name}!"
                
            user_profile = f"""
            -Country: {event.data.get('country', 'N/A')}
            -Investment goals: {event.data.get('investment_goals', 'N/A')}
            -Risk tolerance: {event.data.get('risk_tolerance', 'N/A')}
            -Preferred industries: {event.data.get('preferred_industries', 'N/A')}
            """
            
            prompt = PERSONALIZED_WELCOME_EMAIL_PROMPT.replace("{{userProfile}}", user_profile)
            
            try:
                print("  👉 [AI] Calling Gemini API...")
                model = genai.GenerativeModel("gemini-1.5-flash")
                response = await model.generate_content_async(prompt)
                print("  ✅ [AI] Generation success")
                return response.text
            except Exception as e:
                 print(f"  ❌ [AI] Generation Error: {e}")
                 return f"Welcome to Tradeshift, {first_name}!"

        # Step 1: Generate AI Intro
        print("👉 [STEP 1] Requesting AI Intro...")
        intro_text = await step.run("generate-welcome-intro", generate_intro)
        print(f"✅ [STEP 1] AI Intro received (len: {len(intro_text)})")

        # Step 2: Send Email (FastAPI-Mail)
        async def send_email():
            print("  👉 [EMAIL] send_email started")
            if not email_address:
                print("  ⚠️ [EMAIL] No email provided, skipping")
                return {"status": "skipped", "reason": "No email provided"}

            # Load the template
            print("  👉 [EMAIL] Loading template...")
            from app.inngest.email_templates import WELCOME_EMAIL_TEMPLATE
            
            # Replace placeholders
            html_content = WELCOME_EMAIL_TEMPLATE.replace("{{name}}", first_name).replace("{{intro}}", intro_text)

            message = MessageSchema(
                subject="Welcome to TradeSim! 🚀",
                recipients=[email_address],
                body=html_content,
                subtype=MessageType.html
            )

            print(f"  👉 [EMAIL] Connecting to SMTP {conf.MAIL_SERVER}:{conf.MAIL_PORT}...")
            fm = FastMail(conf)
            try:
                await fm.send_message(message)
                print(f"  ✅ [EMAIL] Sent successfully to {email_address}")
                return {"status": "sent", "email": email_address}
            except Exception as e:
                print(f"  ❌ [EMAIL] Failed to send: {e}")
                # SWALLOW EXCEPTION to prevent Inngest from retrying infinitely
                # We return a failure status so the step relies on this result
                return {"status": "failed", "error": str(e)}
        
        print("👉 [STEP 2] Requesting Email Send...")
        await step.run("send-welcome-email", send_email)
        print("✅ [STEP 2] Email step completed")

        return {
            "success": True,
            "message": f"Welcome email processed for {email_address}"
        }
    except Exception as e:
        print(f"🔥 CRITICAL INNGEST ERROR: {e}")
        import traceback
        traceback.print_exc()
        # We still raise critical top-level errors, but email-specific ones are handled above
        raise e
