import inngest
import os

# Set Inngest to Dev Mode to allow local Dev Server sync without signing key
os.environ["INNGEST_DEV"] = "1"
# Remove dummy signing key so SDK doesn't expect Cloud signatures
if "INNGEST_SIGNING_KEY" in os.environ:
    del os.environ["INNGEST_SIGNING_KEY"]

inngest_client = inngest.Inngest(
    app_id="tradeshift",
)
