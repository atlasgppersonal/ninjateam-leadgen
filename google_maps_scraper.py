import asyncio
from playwright.async_api import async_playwright
import json
import re
import google.generativeai as genai
import os
import sys

# --- Helper Functions ---
def normalize_phone_number(phone):
    if not phone: return None
    cleaned = re.sub(r'\D', '', phone)
    if len(cleaned) == 11 and cleaned.startswith('1'):
        cleaned = cleaned[1:]
    return cleaned if len(cleaned) == 10 else None

def clean_website_url(url):
    if not url: return None
    # Remove http(s):// and www.
    cleaned_url = re.sub(r'^(https?://)?(www\.)?', '', url)
    # Remove trailing slash
    cleaned_url = cleaned_url.rstrip('/')
    return cleaned_url

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

# Hardcoded LLM prompt and API key for testing purposes
# In a production environment, these would typically be loaded from a config file or environment variables.
GOOGLE_API_KEY = "AIzaSyAu6MUOthXxuvH0uWNYdpguAIvEsz6Zk38" # User provided API Key
LLM_MODEL = "gemini-2.0-flash-lite"

EXTRACT_GMAPS_BUSINESS_DETAILS_PROMPT = """You are an expert data extraction system. I will provide a JSON array where each object represents a text snippet from a Google Maps search result or a detailed business page. Your task is to analyze each 'text_content' and extract the required information.

CRITICAL RULES:
1. Your response MUST be a single, valid JSON array.
2. The response array MUST contain one object for every object in the input array.
3. Each object in your response array MUST include the original 'snippet_id' and the following extracted keys: 'business_name', 'phone', 'website_url', 'stars', 'number_of_reviews', 'has_posts', and 'detailed_metadata'.
4. Maintain the exact same order as the input array.
5. If a specific piece of information cannot be found, you MUST return null for that key.
6. The 'phone' key MUST always be a string or null. Extract only 10-digit phone numbers.
7. 'stars' should be a string (e.g., '5.0', '4.5') or null. Look for ratings like "X.Y stars" or "X out of 5 stars".
8. 'number_of_reviews' should be a string (e.g., '3', '64') or null. Look for numbers associated with "reviews" or "ratings".
9. 'has_posts' should be a boolean (true if "updates", "From the owner", or "Questions and answers" sections are explicitly mentioned or clearly present, false otherwise) or null if not determinable. Specifically, look for the presence of sections or phrases like "Add update", "Post an update", "Add offer", "Create an offer", "Add event", "Let customers know about events". Crucially, EXCLUDE "Updates from customers". If these specific sections are not mentioned or are not clearly indicated as active/present, assume false.
10. 'detailed_metadata' should be a JSON object containing any other relevant key-value pairs found in the text (e.g., address, hours, services, accessibility, etc.), or null if no additional metadata is found. Extract these as key-value pairs where the key is a descriptive string and the value is the extracted data. Be thorough in capturing all available details. For example, if hours are listed, extract them as "hours": "Mon-Fri: 9am-5pm". If a website is mentioned in detailed metadata, ensure it's captured in the 'website_url' field.

Here is the batch of snippets to process: {batch_data_json}
Your JSON Array Response:"""


async def find_business_on_maps(page, contact_data: dict, headless: bool = False) -> list:
    """
    Searches Google Maps for a business based on provided contact data (website, then name).
    Extracts details (name, phone, website, stars, reviews) using LLM.
    Returns a list of extracted business details.
    """
    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel(LLM_MODEL)
    
    try:
        search_query = ""
        if contact_data.get('website_url'):
            cleaned_url = clean_website_url(contact_data['website_url'])
            search_query = cleaned_url
            print(f"Attempting to search by website URL: {search_query}")
        elif contact_data.get('business_name'):
            search_query = contact_data['business_name'] + " " + contact_data.get('City', '') # Add city for better search
            print(f"Attempting to search by business name: {search_query}")
        else:
            print("No valid search criteria (website_url or business_name) provided.")
            return []

        # Construct Google Maps search URL
        maps_search_url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
        print(f"Navigating to: {maps_search_url}")
        await page.goto(maps_search_url, wait_until="domcontentloaded")

        print("Waiting for a general page element to load (e.g., search input)...")
        # Wait for a more general element that indicates the page has loaded
        await page.wait_for_selector('input#searchboxinput', state='visible', timeout=20000) 

        print("Extracting entire page body text content for LLM processing...")
        # Extract the entire text content of the body element
        page_text_content = await page.locator('body').text_content()
        
        if page_text_content:
            print("\n--- Extracted Text Content for LLM ---")
            print(page_text_content[:1000] + "...") # Print first 1000 chars
            print("--------------------------------------")

            # Prepare data for LLM
            llm_input_data = [{
                "snippet_id": "google_maps_page_body", # A dummy ID for the LLM
                "text_content": page_text_content # Pass the entire extracted text
            }]
            
            prompt = EXTRACT_GMAPS_BUSINESS_DETAILS_PROMPT.format(batch_data_json=json.dumps(llm_input_data))
            
            print("\n--- Sending content to LLM for extraction ---")
            response = await model.generate_content_async(prompt)
            
            cleaned_text = response.text.strip().replace("```json", "").replace("```", "").strip()
            extracted_data_list = json.loads(cleaned_text)

            final_extracted_businesses = []
            for extracted_item in extracted_data_list:
                # Add a check to ensure extracted_item is a dictionary before accessing keys
                if not isinstance(extracted_item, dict):
                    print(f"Warning: Skipping invalid item in LLM response: {extracted_item}")
                    continue

                extracted_phone = extracted_item.get('phone')
                normalized_phone = normalize_phone_number(extracted_phone)
                
                # Ensure website_url is extracted correctly from detailed_metadata if not directly found
                website_url = extracted_item.get('website_url')
                if not website_url and extracted_item.get('detailed_metadata', {}).get('website'):
                    website_url = extracted_item['detailed_metadata']['website']
                
                final_extracted_businesses.append({
                    "business_name": extracted_item.get('business_name'),
                    "phone": normalized_phone,
                    "website_url": website_url,
                    "stars": extracted_item.get('stars'),
                    "number_of_reviews": extracted_item.get('number_of_reviews'),
                    "has_posts": extracted_item.get('has_posts'),
                    "detailed_metadata": extracted_item.get('detailed_metadata')
                })
            
            print("\n--- LLM Extracted Details for all results ---")
            for i, business in enumerate(final_extracted_businesses):
                print(f"Result {i+1}:")
                print(f"  Business Name: {business.get('business_name', 'N/A')}")
                print(f"  Phone: {business.get('phone', 'N/A')}")
                print(f"  Website: {business.get('website_url', 'N/A')}")
                print(f"  Stars: {business.get('stars', 'N/A')}")
                print(f"  Reviews: {business.get('number_of_reviews', 'N/A')}")
                print(f"  Has Posts: {business.get('has_posts', 'N/A')}")
                print(f"  Detailed Metadata: {json.dumps(business.get('detailed_metadata', {}), indent=2)}")
            print("---------------------------------------------")

            return final_extracted_businesses

        else:
            print("Could not extract text content from the search results page.")
            return []

    except Exception as e:
        print(f"An error occurred during scraping or LLM processing: {e}")
        import traceback
        traceback.print_exc()
        return []


async def get_detailed_business_info(page, business_maps_url: str) -> dict:
    """
    Navigates to a specific Google Maps business URL and extracts detailed information.
    """
    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel(LLM_MODEL)
    
    print(f"Navigating to detailed business page: {business_maps_url}")
    await page.goto(business_maps_url, wait_until="domcontentloaded")

    print("Waiting for detailed business info (role='main') to load...")
    await page.wait_for_selector('div[role="main"]', state='visible', timeout=20000)
    
    detailed_content = await page.locator('div[role="main"]').text_content()

    if detailed_content:
        print("\n--- Extracted Detailed Content for LLM ---")
        print(detailed_content[:1000] + "...") # Print first 1000 chars
        print("------------------------------------------")

        llm_input_data = [{
            "snippet_id": "google_maps_detailed_page",
            "text_content": detailed_content
        }]
        
        prompt = EXTRACT_GMAPS_BUSINESS_DETAILS_PROMPT.format(batch_data_json=json.dumps(llm_input_data))
        
        print("\n--- Sending detailed content to LLM for extraction ---")
        response = await model.generate_content_async(prompt)
        
        cleaned_text = response.text.strip().replace("```json", "").replace("```", "").strip()
        extracted_data_list = json.loads(cleaned_text)

        if extracted_data_list:
            extracted_item = extracted_data_list[0] # Assuming only one item for detailed page
            normalized_phone = normalize_phone_number(extracted_item.get('phone'))
            
            # Ensure website_url is captured if it's in detailed_metadata
            website_url = extracted_item.get('website_url')
            if not website_url and extracted_item.get('detailed_metadata', {}).get('website'):
                website_url = extracted_item['detailed_metadata']['website']
            
            return {
                "business_name": extracted_item.get('business_name'),
                "phone": normalized_phone,
                "website_url": website_url,
                "stars": extracted_item.get('stars'),
                "number_of_reviews": extracted_item.get('number_of_reviews'),
                "has_posts": extracted_item.get('has_posts'),
                "detailed_metadata": extracted_item.get('detailed_metadata')
            }
    return {}

async def test_website_search_scenario(page):
    contact_to_find = {
        "business_name": "PRO WELDING FABRICATORS, LLC", # Included for context, but search by website
        "website_url": "pwfflorida.com",
        "City": "Orlando,Florida"
    }

    print(f"\n--- Testing Website Search Scenario for: {contact_to_find['website_url']} ---")
    found_businesses = await find_business_on_maps(page, contact_to_find, headless=False)

    if found_businesses:
        print("\n--- Website Search Result ---")
        # Assuming only one primary result for website search
        business = found_businesses[0]
        print(f"  Business Name: {business.get('business_name', 'N/A')}")
        print(f"  Phone: {business.get('phone', 'N/A')}")
        print(f"  Website: {business.get('website_url', 'N/A')}")
        print(f"  Stars: {business.get('stars', 'N/A')}")
        print(f"  Reviews: {business.get('number_of_reviews', 'N/A')}")
        print(f"  Has Posts: {business.get('has_posts', 'N/A')}")
        print(f"  Detailed Metadata: {json.dumps(business.get('detailed_metadata', {}), indent=2)}")
        print("-----------------------------")
    else:
        print("No businesses found for website search.")

async def test_business_name_search_scenario(page):
    contact_to_find = {
        "business_name": "PRO WELDING FABRICATORS, LLC",
        "City": "Orlando,Florida"
    }

    print(f"\n--- Testing Business Name Search Scenario for: {contact_to_find['business_name']} ---")
    
    search_query = contact_to_find['business_name'] + " " + contact_to_find.get('City', '')
    maps_search_url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
    print(f"Navigating to: {maps_search_url}")
    await page.goto(maps_search_url, wait_until="domcontentloaded")
    await page.wait_for_selector('input#searchboxinput', state='visible', timeout=20000)

    # Extract initial search results (titles and links)
    # This part is tricky as Google Maps search results can vary in structure.
    # We'll try to find common elements for business listings.
    # A common pattern is a link with a specific role or data-attribute.
    # For now, let's try to find links that likely lead to business details.
    # This selector might need adjustment based on actual Google Maps HTML.
    business_links = await page.locator('a[aria-label*="View details for"], a[data-item-id*="place"]').all()
    
    if not business_links:
        print("No individual business links found on the search results page. Attempting to extract from main page content.")
        # Fallback to extracting from the main page content if no specific links are found
        page_text_content = await page.locator('body').text_content()
        if page_text_content:
            prompt = EXTRACT_GMAPS_BUSINESS_DETAILS_PROMPT.format(batch_data_json=json.dumps([{"snippet_id": "google_maps_search_page_fallback", "text_content": page_text_content}]))
            model = genai.GenerativeModel(LLM_MODEL)
            response = await model.generate_content_async(prompt)
            cleaned_text = response.text.strip().replace("```json", "").replace("```", "").strip()
            extracted_data_list = json.loads(cleaned_text)
            
            print("\n--- LLM Extracted Details from Search Page (Fallback) ---")
            for i, business in enumerate(extracted_data_list):
                print(f"Result {i+1}:")
                print(f"  Business Name: {business.get('business_name', 'N/A')}")
                print(f"  Phone: {business.get('phone', 'N/A')}")
                print(f"  Website: {business.get('website_url', 'N/A')}")
                print(f"  Stars: {business.get('stars', 'N/A')}")
                print(f"  Reviews: {business.get('number_of_reviews', 'N/A')}")
                print(f"  Has Posts: {business.get('has_posts', 'N/A')}")
                print(f"  Detailed Metadata: {json.dumps(business.get('detailed_metadata', {}), indent=2)}")
            print("---------------------------------------------------------")
        else:
            print("Could not extract text content from the search results page for fallback.")
        return

    print(f"Found {len(business_links)} potential business links.")
    
    # Click on the first business link to get detailed info
    if business_links:
        first_link = business_links[0]
        business_maps_url = await first_link.get_attribute('href')
        if business_maps_url:
            print(f"Clicking on the first business link: {business_maps_url}")
            # It's better to navigate directly to the URL than to click,
            # as clicking can sometimes be flaky or open in a new tab.
            detailed_info = await get_detailed_business_info(page, business_maps_url)
            if detailed_info:
                print("\n--- Detailed Info from First Business Link ---")
                print(f"  Business Name: {detailed_info.get('business_name', 'N/A')}")
                print(f"  Phone: {detailed_info.get('phone', 'N/A')}")
                print(f"  Website: {detailed_info.get('website_url', 'N/A')}")
                print(f"  Stars: {detailed_info.get('stars', 'N/A')}")
                print(f"  Reviews: {detailed_info.get('number_of_reviews', 'N/A')}")
                print(f"  Has Posts: {detailed_info.get('has_posts', 'N/A')}")
                print(f"  Detailed Metadata: {json.dumps(detailed_info.get('detailed_metadata', {}), indent=2)}")
                print("----------------------------------------------")
            else:
                print("Failed to extract detailed info from the first business link.")
        else:
            print("Could not get href from the first business link.")
    else:
        print("No business links found to click on.")


if __name__ == "__main__":
    async def main():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()
            await test_website_search_scenario(page)
            await test_business_name_search_scenario(page)
            await browser.close()
    asyncio.run(main())
