import asyncio
from playwright.async_api import async_playwright, TimeoutError

async def test_email_button_selectors():
    target_url = "https://tampa.craigslist.org/hil/lbs/d/tampa-cheapest-around-town-junk/7873375957.html"
    
    # Define the selectors to test for the 'email' button
    email_button_selectors = {
        "Selector 1 (Specific Text Match)": 'button.reply-option-header:has(span.reply-option-label:text-is("email"))',
        "Selector 2 (Class Only)": 'button.reply-option-header',
        "Selector 3 (Span Text Parent Button)": 'span.reply-option-label:text-is("email")', # Will get parent button
        "Selector 4 (Role Button Name)": 'button[role="button"]:has-text("email")'
    }
    
    # Selector for the div that appears after the email button is clicked
    email_info_selector = '.reply-info.js-only'
    
    print(f"--- Testing Email Button Selectors on {target_url} ---")

    async with async_playwright() as p:
        for name, selector in email_button_selectors.items():
            print(f"\nTesting: {name} ('{selector}')")
            browser = None # Initialize browser to None for proper cleanup
            try:
                browser = await p.chromium.launch(headless=False) # Keep headless=False for visual debugging
                context = await browser.new_context()
                page = await context.new_page()
                
                await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                print(f"    -> Navigated to {target_url}")

                # Click the initial 'reply' button if it exists
                try:
                    await page.locator('button.reply-button').click(timeout=10000)
                    print("    -> Clicked initial 'reply' button.")
                except TimeoutError:
                    print("    -> Initial 'reply' button not found or not clickable within timeout. Proceeding...")
                
                print(f"    -> Waiting for email button to become actionable using '{selector}'...")
                
                # Handle Selector 3 specifically to click the parent button
                if name == "Selector 3 (Span Text Parent Button)":
                    email_span_locator = page.locator(selector)
                    await email_span_locator.wait_for(state='visible', timeout=300000)
                    await email_span_locator.click() # Click the span, hoping event bubbles to button
                    print(f"    -> Clicked span with text 'email'.")
                else:
                    # Use Playwright's native click for other selectors
                    await page.locator(selector).click(timeout=300000)
                    print(f"    -> Clicked email button using '{selector}'.")

                # Post-Click Verification Loop with Retries
                max_retries = 5
                retry_delay_ms = 200
                found_email_info = False
                for i in range(max_retries):
                    try:
                        print(f"    -> Attempt {i+1}/{max_retries}: Verifying email info div visibility...")
                        await page.wait_for_selector(email_info_selector, state='visible', timeout=retry_delay_ms)
                        found_email_info = True
                        print(f"    -> SUCCESS: Email info div is now visible for '{name}'.")
                        break # Exit loop if successful
                    except TimeoutError:
                        print(f"    -> Email info div not visible yet. Retrying in {retry_delay_ms}ms...")
                        await asyncio.sleep(retry_delay_ms / 1000) # Convert ms to seconds
                
                if found_email_info:
                    print(f"RESULT: '{name}' SUCCEEDED!")
                else:
                    print(f"RESULT: '{name}' FAILED: Email info div did not appear after {max_retries} retries.")

            except TimeoutError as e:
                print(f"RESULT: '{name}' FAILED: Timeout during interaction. Error: {e}")
            except Exception as e:
                print(f"RESULT: '{name}' FAILED: An unexpected error occurred. Error: {e}")
            finally:
                if browser:
                    await browser.close()
                    print("    -> Browser closed.")

asyncio.run(test_email_button_selectors())
