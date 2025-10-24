import asyncio
import os
from playwright.async_api import async_playwright

# Define the path for the user data directory
# This directory will store the browser profile, including installed extensions.
# Define the path for the user data directory
# This directory will store the browser profile, including installed extensions.
# Define the path for the user data directory
# This directory will store the browser profile, including installed extensions.
USER_DATA_DIR = r"C:\Users\GIO PLUGLIESE\AppData\Local\Google\Chrome\User Data"

async def run_seo_arbitrage_strategy():
    """
    Runs the SEO arbitrage strategy using the pre-configured browser profile.
    """
    if not os.path.exists(USER_DATA_DIR):
        print(f"Error: Chrome User Data directory '{USER_DATA_DIR}' not found.")
        print("Please ensure the specified Chrome User Data directory exists at this path.")
        return

    print(f"Launching browser with existing profile 'Profile 10' from: {os.path.abspath(USER_DATA_DIR)}")
    async with async_playwright() as p:
        browser = await p.chromium.launch_persistent_context(
            user_data_dir=USER_DATA_DIR,
            headless=False,   # must be False for extensions to load
            args=[
                "--disable-blink-features=AutomationControlled",
                "--profile-directory=Profile 10", # Explicitly specify Profile 10
                # optionally set a profile-specific user-agent if needed:
                # "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36"
            ],
            ignore_default_args=["--enable-automation"]  # helps reduce bot fingerprinting
        )
        # Keep the browser open until manually closed by the user
        await browser.wait_for_event("close")
        print("Browser closed. SEO arbitrage strategy run complete.")

async def main():
    await run_seo_arbitrage_strategy()

if __name__ == "__main__":
    asyncio.run(main())
