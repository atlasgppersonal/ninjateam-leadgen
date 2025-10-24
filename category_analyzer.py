import asyncio
import json
import re
import os
import sqlite3
from collections import defaultdict
from playwright.async_api import async_playwright

# --- HELPER FUNCTIONS (Copied from contact-extractor.py) ---
def remove_emojis(text: str) -> str:
    if not text: return ""
    emoji_pattern = re.compile(r'['
                               u'\U0001F600-\U0001F64F\U0001F300-\U0001F5FF'
                               u'\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF'
                               u'\u2600-\u26FF\u2700-\u27BF\u2580-\u259F\uFE0F\u3030'
                               ']+', flags=re.UNICODE)
    return emoji_pattern.sub(r'', text).strip()

# --- ADAPTED HARVESTER FUNCTION ---
async def harvester_for_analysis(headless_browser, config):
    print("    [Harvester for Analysis] Starting a new run...")
    print(f"    [Harvester for Analysis] Initializing Playwright...")
    page = None
    all_raw_posts = []
    all_category_map = {}
    all_image_hash_map = {}

    try:
        producer_cfg = config['producer_settings']
        
        # We don't need to load processed hashes from DB for this analysis script
        # with sqlite3.connect(producer_cfg['master_database_file']) as con:
        #     cur = con.cursor()
        #     cur.execute("SELECT image_hash FROM contacts WHERE image_hash IS NOT NULL")
        #     processed_hashes = {row[0] for row in cur.fetchall()}
        # print(f"    [Harvester] Loaded {len(processed_hashes)} existing image hashes from DB for duplicate checking.")
        
        state = {"min_post_id": None, "category_map": {}, "image_hash_map": {}, "raw_posts": []}
        
        page = await headless_browser.new_page()
        
        # Define a local handler for this specific run
        async def handle_search_response_local(response):
            try:
                url = response.url
                if "full?batch" in url and "0-360" not in url:
                    if state["min_post_id"]: return # Only process the first 'full' batch
                    data = await response.json()
                    state["min_post_id"] = data['data']['decode']['minPostingId']
                    for item in data['data']['items']:
                        base_id = item[0]
                        if len(item) > 2: state["category_map"][base_id] = item[2]
                        if len(item) > 5 and item[5] and item[5] != '0': state["image_hash_map"][base_id] = item[5]
                elif "batch?batch" in url:
                    if not state["category_map"]: return
                    data = await response.json()
                    for item in data.get('data', {}).get('batch', []):
                        try:
                            base_id, url_slug = item[0], next((el[1] for el in item if isinstance(el, list) and len(el) == 2 and el[0] == 6), None)
                            post_id_num = state['min_post_id'] + base_id
                            state["raw_posts"].append({"base_id": base_id, "url_slug": url_slug, "post_id_num": post_id_num})
                        except (IndexError, TypeError): continue
            except Exception as e:
                print(f"!!! [Harvester/Network] ERROR parsing response from {response.url}. Error: {e}")

        page.on("response", handle_search_response_local)
        
        sub_domain = config['producer_settings']['craigslist_sub_domain'] 
        base_url = producer_cfg['craigslist_base_url']
        search_path = producer_cfg['craigslist_services_path']
        search_url = f"https://{sub_domain}.{base_url}{search_path}"
        print(f"    [Harvester for Analysis] >>> Navigating to primary search URL: {search_url}")
        await page.goto(search_url, wait_until="domcontentloaded")
        
        # Scroll to load more posts
        for i in range(15): # Reduced scrolls for quicker analysis, adjust as needed
            print(f"    [Harvester for Analysis] Scrolling... ({i+1}/15)")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2) # Give time for network responses

        await page.close()
        print(f"    [Harvester for Analysis] Network scan complete. Found a total of {len(state['raw_posts'])} raw posts from traffic.")
        
        # Process raw posts to get category names
        processed_posts_with_categories = []
        for post_data in state["raw_posts"]:
            category_id = state['category_map'].get(post_data['base_id'])
            if category_id:
                abbr, cat_name = producer_cfg['category_mapping'].get(str(category_id), (None, "General"))
                if cat_name != "General": # Only include posts with a specific category
                    processed_posts_with_categories.append({"post_id": post_data['post_id_num'], "original_category": cat_name})
        
        return processed_posts_with_categories

    except Exception as e:
        print(f"!!! [Harvester for Analysis] An error occurred: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        if page:
            await page.close()

# --- MAIN ANALYSIS LOGIC ---
async def main_analysis():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_filename = os.path.join(script_dir, 'config.json')
    
    try:
        with open(config_filename, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"CRITICAL ERROR: '{config_filename}' file not found.")
        return
    except json.JSONDecodeError:
        print("CRITICAL ERROR: Could not parse config file.")
        return

    city_profiles = config['city_profiles']
    producer_cfg = config['producer_settings']

    all_cities_category_counts = {}

    async with async_playwright() as p:
        # Launch headless browser (Firefox) for background tasks
        headless_browser = await p.firefox.launch(headless=True)
        print("--- [Main Analysis] Headless Firefox browser launched for scraping.")

        try:
            for city_name, city_data in city_profiles.items():
                print(f"\n--- Analyzing categories for city: {city_name.upper()} ---")
                
                # Temporarily set the sub_domain for the current city
                config['producer_settings']['craigslist_sub_domain'] = city_name
                
                # Run the adapted harvester
                posts_with_categories = await harvester_for_analysis(headless_browser, config)
                
                category_counts = defaultdict(int)
                for post in posts_with_categories:
                    category_counts[post['original_category']] += 1
                
                all_cities_category_counts[city_name] = category_counts
                
                print(f"--- Finished analyzing {city_name.upper()}. ---")

        finally:
            await headless_browser.close()
            print("--- [Main Analysis] Headless browser closed.")

    # Generate and print the report
    print("\n" + "="*50)
    print("           CATEGORY AUDIENCE POTENTIAL REPORT")
    print("="*50)

    for city, counts in all_cities_category_counts.items():
        print(f"\n--- City: {city.upper()} ---")
        if not counts:
            print("No categories found for this city.")
            continue
        
        # Sort categories by count in descending order
        sorted_categories = sorted(counts.items(), key=lambda item: item[1], reverse=True)
        
        for category, count in sorted_categories:
            print(f"- {category}: {count} posts")
    
    print("\n" + "="*50)
    print("Report Generation Complete.")
    print("="*50)

if __name__ == "__main__":
    asyncio.run(main_analysis())
