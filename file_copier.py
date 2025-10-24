import asyncio
import os
from playwright.async_api import async_playwright, Page
import re

# Selectors
FILE_TREE_ITEM_SELECTOR = 'div.group.flex.cursor-pointer.items-center.px-2.py-1.text-sm'
DIRECTORY_ICON_SELECTOR = 'div.mr-1.flex-shrink-0 > svg' # This SVG indicates a directory
FILE_NAME_SPAN_SELECTOR = 'span.flex-1.truncate'
FILE_CONTENT_SELECTOR = 'div.cm-content'

async def create_local_file(path, content):
    """Creates a local file with the given content, ensuring directories exist."""
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Saved: {path}")

async def traverse_and_copy(page: Page, base_path=".", current_depth=0):
    """
    Traverses the file tree, clicks on files, extracts content, and saves it locally.
    """
    # Get all potential items
    # A more robust way to get items at the current depth might be needed if margin-left is unreliable.
    # For now, let's assume margin-left is consistent.
    
    # First, get all potential items
    all_items_locators = await page.locator(FILE_TREE_ITEM_SELECTOR).all()
    
    current_level_items = []
    for item_locator in all_items_locators:
        # Get the margin-left of the first div inside the item, which indicates depth
        margin_div = await item_locator.locator('div.mr-1.flex-shrink-0').first.get_attribute('style')
        
        # Extract margin-left value, default to 0 if not found or not a number
        margin_left_match = re.search(r'margin-left:\s*(\d+)px', margin_div if margin_div else '')
        item_depth = int(margin_left_match.group(1)) // 16 if margin_left_match else 0 # Assuming 16px per depth level

        if item_depth == current_depth:
            current_level_items.append(item_locator)

    for item_locator in current_level_items:
        item_name = await item_locator.locator(FILE_NAME_SPAN_SELECTOR).text_content()
        local_path = os.path.join(base_path, item_name)
        
        is_directory = await item_locator.locator(DIRECTORY_ICON_SELECTOR).count() > 0

        if is_directory:
            print(f"Entering directory: {local_path}")
            # Check if directory is collapsed (SVG does not have rotate-90 class)
            icon_svg = item_locator.locator(DIRECTORY_ICON_SELECTOR)
            is_collapsed = await icon_svg.evaluate("node => !node.classList.contains('rotate-90')")

            if is_collapsed:
                await item_locator.click() # Expand the directory
                await asyncio.sleep(0.5) # Give time for expansion
            
            # Recursively traverse the subdirectory
            await traverse_and_copy(page, local_path, current_depth + 1)
            
            # Collapse the directory after traversing (optional)
            if is_collapsed:
                await item_locator.click()
                await asyncio.sleep(0.5)
        else:
            print(f"Copying file: {local_path}")
            
            await item_locator.click()
            await asyncio.sleep(1) # Give time for content to load/update

            try:
                content_element = page.locator(FILE_CONTENT_SELECTOR)
                # Wait for the content element to be visible and potentially for its content to change
                await content_element.wait_for(state='visible', timeout=10000)
                file_content = await content_element.text_content()
                await create_local_file(local_path, file_content)
            except Exception as e:
                print(f"Error extracting content for {item_name}: {e}")
                await create_local_file(local_path, f"ERROR: Could not extract content. {e}")

async def main():
    url = "https://lovable.dev/projects/2c04567f-cbd5-4a39-82e3-6e6400c2b98a"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        print(f"Navigating to {url}...")
        await page.goto(url)
        
        print("Please log in and navigate to the project page with the file tree.")
        print("The script will wait for the file tree to become visible.")
        
        # Wait for the file tree container to be visible
        # This will pause execution until the user has logged in and navigated to the correct page
        FILE_TREE_CONTAINER_ID = '#radix-_r_f1_-content-files'
        print(f"Waiting for element '{FILE_TREE_CONTAINER_ID}' to be visible...")
        await page.wait_for_selector(FILE_TREE_CONTAINER_ID, state='visible', timeout=0) # timeout=0 means wait indefinitely
        print(f"Element '{FILE_TREE_CONTAINER_ID}' is now visible. Proceeding with file copying.")
        
        print("Starting file tree traversal and content extraction...")
        await traverse_and_copy(page)
        
        print("File copying complete. Closing browser.")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
