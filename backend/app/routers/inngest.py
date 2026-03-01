from fastapi import APIRouter
import inngest.fast_api
from app.inngest.client import inngest_client
from app.inngest.functions import send_signup_email

import logging

# Enable debug logging for Inngest
logging.getLogger("inngest").setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)

router = APIRouter()
print("🛠️ Inngest Router Loaded")

inngest.fast_api.serve(
    app=router,
    client=inngest_client,
    functions=[send_signup_email],
)
