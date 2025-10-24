import re
import json

# The use_mcp_tool is a globally available function provided by the environment,
# so it does not need to be imported from a specific library.
# Removed the incorrect import: from some_mcp_library import use_mcp_tool

# Mock implementation of use_mcp_tool for local execution and testing
# In a real environment, this function would be provided by the system.
def use_mcp_tool(server_name, tool_name, arguments):
    """
    Mock implementation of use_mcp_tool for simulating MCP tool calls.
    This mock returns predefined results based on the 'query' argument.
    """
    print(f"Mock use_mcp_tool called: server={server_name}, tool={tool_name}, args={arguments}")
    keyword = arguments.get("query")
    
    # Simulate SERP results for specific keywords
    if keyword == "mobile welding Orlando":
        return [
            {'title': 'Tony\'s Welding - Best Welding Services in Orlando', 'url': 'https://tonywelding.com', 'rank': 1},
            {'title': 'Orlando Fab Works | Custom Metal Fabrication', 'url': 'https://orlandofab.com', 'rank': 2},
            {'title': 'PRO WELDING FABRICATORS, LLC - Mobile Welding in Orlando', 'url': 'http://pwfflorida.com', 'rank': 3},
            {'title': 'Gate Repair Orlando | Superior Gates', 'url': 'https://superiorgates.com', 'rank': 4},
            {'title': 'Affordable Welding Services Orlando', 'url': 'https://affordablewelding.com', 'rank': 5},
            {'title': 'Emergency Welding Services - 24/7 Availability', 'url': 'https://emergencywelding.com', 'rank': 6},
            {'title': 'Best Welding Shops in Orlando', 'url': 'https://bestweldingorlando.com', 'rank': 7},
            {'title': 'Welding Service Near Me', 'url': 'https://weldingservicenearme.com', 'rank': 8},
            {'title': '24/7 Welding Solutions Orlando', 'url': 'https://247welding.com', 'rank': 9},
            {'title': 'Local Welding Experts', 'url': 'https://localweldingexperts.com', 'rank': 10}
        ]
    elif keyword == "gate repair Orlando":
         return [
            {'title': 'Gate Repair Orlando | Superior Gates', 'url': 'https://superiorgates.com', 'rank': 1},
            {'title': 'Orlando Fab Works - Gate Fabrication', 'url': 'https://orlandofab.com', 'rank': 2},
            {'title': 'PRO WELDING FABRICATORS, LLC - Gate Repair', 'url': 'http://pwfflorida.com', 'rank': 3},
            {'title': 'Tony\'s Welding - Gate Services', 'url': 'https://tonywelding.com', 'rank': 4},
            {'title': 'Affordable Gate Repair Orlando', 'url': 'https://affordablegaterepair.com', 'rank': 5},
            {'title': 'Emergency Gate Repair Orlando', 'url': 'https://emergencygaterepair.com', 'rank': 6},
            {'title': 'Best Gate Repair Orlando', 'url': 'https://bestgaterepairorlando.com', 'rank': 7},
            {'title': 'Gate Repair Near Me', 'url': 'https://gaterepairnearme.com', 'rank': 8},
            {'title': '24/7 Gate Repair Orlando', 'url': 'https://247gaterepair.com', 'rank': 9},
            {'title': 'Local Gate Repair Experts', 'url': 'https://localgaterepair.com', 'rank': 10}
        ]
    else: # Default for other keywords
        return [
            {'title': f'Result 1 for {keyword}', 'url': 'https://example.com/result1', 'rank': 1},
            {'title': f'Result 2 for {keyword}', 'url': 'https://example.com/result2', 'rank': 2},
            {'title': f'Client Site for {keyword}', 'url': 'http://pwfflorida.com', 'rank': 3}, # Simulate client ranking
            {'title': f'Result 4 for {keyword}', 'url': 'https://example.com/result4', 'rank': 4},
            {'title': f'Result 5 for {keyword}', 'url': 'https://example.com/result5', 'rank': 5},
            {'title': f'Result 6 for {keyword}', 'url': 'https://example.com/result6', 'rank': 6},
            {'title': f'Result 7 for {keyword}', 'url': 'https://example.com/result7', 'rank': 7},
            {'title': f'Result 8 for {keyword}', 'url': 'https://example.com/result8', 'rank': 8},
            {'title': f'Result 9 for {keyword}', 'url': 'https://example.com/result9', 'rank': 9},
            {'title': f'Result 10 for {keyword}', 'url': 'https://example.com/result10', 'rank': 10}
        ]

def generate_keywords(contact, competitors):
    """
    Generates 10 keyword variations for SEO analysis based on contact and competitor data.

    Args:
        contact (dict): Dictionary containing client information.
                        Expected keys: 'category', 'City', 'services_rendered', 'website_url', 'post_id'.
        competitors (list): List of competitor dictionaries.
                            Expected keys: 'name', 'url'.

    Returns:
        list: A list of 10 keyword strings.
    """
    category = contact.get('category', '')
    city_full = contact.get('City', '')
    # Extract city name, assuming format like "City, State" or just "City"
    city = city_full.split(',')[0].strip() if city_full else ''
    services = contact.get('services_rendered', [])
    client_site = contact.get('website_url', '')
    contact_id = contact.get('post_id', 'unknown_id')

    keywords = []

    # 1. category + city
    if category and city:
        keywords.append(f"{category} {city}")

    # Select a few services for specific keywords
    # Ensure we have enough services, otherwise use placeholders or skip
    selected_services = []
    if len(services) >= 3:
        selected_services = [services[0], services[1], services[2]] # e.g., "Onsite Mobile Welding", "Gate fabrication and repair", "Patio work"
    elif len(services) == 2:
        selected_services = [services[0], services[1], "welding"] # Add a generic term if only 2 services
    elif len(services) == 1:
        selected_services = [services[0], "welding", "fabrication"]
    else:
        selected_services = ["welding", "fabrication", "repair"] # Fallback if no services

    # 2. service1 + city
    if selected_services and city:
        keywords.append(f"{selected_services[0]} {city}")

    # 3. service2 + city
    if len(selected_services) > 1 and city:
        keywords.append(f"{selected_services[1]} {city}")

    # 4. service3 + city
    if len(selected_services) > 2 and city:
        keywords.append(f"{selected_services[2]} {city}")

    # 5. affordable + service + city
    if selected_services and city:
        keywords.append(f"affordable {selected_services[0]} {city}")

    # 6. emergency + service + city
    if selected_services and city:
        keywords.append(f"emergency {selected_services[0]} {city}")

    # 7. service + 'near me'
    if selected_services:
        keywords.append(f"{selected_services[0]} near me")

    # 8. best + service + city
    if selected_services and city:
        keywords.append(f"best {selected_services[0]} {city}")

    # 9. 24/7 + service + city
    if selected_services and city:
        keywords.append(f"24/7 {selected_services[0]} {city}")

    # 10. competitor name + city
    if competitors and competitors[0].get('name') and city:
        competitor_name = competitors[0]['name']
        # Clean up competitor name if it contains extra info like "LLC" or "Inc." for better keyword matching
        competitor_name_cleaned = re.sub(r',? LLC| Inc\.?|\.', '', competitor_name).strip()
        keywords.append(f"{competitor_name_cleaned} {city}")

    # Ensure we have exactly 10 keywords, padding if necessary (though the logic above should produce 10)
    # This is a safeguard, ideally the logic above is robust.
    while len(keywords) < 10:
        keywords.append(f"placeholder_keyword_{len(keywords) + 1}")

    return keywords[:10] # Return exactly 10 keywords

def analyze_keyword(keyword, client_site, competitors):
    """
    Analyzes a single keyword by scraping SERP results and determining rankings.

    Args:
        keyword (str): The keyword to analyze.
        client_site (str): The client's website URL.
        competitors (list): List of competitor dictionaries {'name': str, 'url': str}.

    Returns:
        dict: A dictionary containing the keyword, client_rank, and competitor ranks.
              Example: {'keyword': 'mobile welding Orlando', 'client_rank': 34, 'competitors': [{'name': 'Tony\'s Welding', 'url': 'tonywelding.com', 'rank': 2}]}
    """
    client_rank = None
    competitor_ranks_map = {} # Use a map to handle potential duplicate competitor names and ensure unique entries
    max_retries = 3
    attempt = 0

    # Normalize client site URL for comparison
    normalized_client_site = re.sub(r'^https?://(www\.)?', '', client_site).lower()
    
    # Pre-normalize competitor URLs
    normalized_competitors = []
    for comp in competitors:
        normalized_comp_url = re.sub(r'^https?://(www\.)?', '', comp.get('url', '')).lower()
        normalized_competitors.append({
            'name': comp.get('name'),
            'url': comp.get('url'),
            'normalized_url': normalized_comp_url
        })

    while attempt < max_retries:
        try:
            # Use the MCP tool to scrape search results
            # The tool should take a 'query' and return a list of {'title': str, 'url': str, 'rank': int}
            search_results = use_mcp_tool(
                server_name="playwright-mcp", # Assuming this server name
                tool_name="scrape_search_results", # Assuming this tool name
                arguments={
                    "query": keyword,
                    "num_results": 10 # Request top 10 results
                }
            )
            
            # Process search_results to find ranks
            if search_results: # Check if results are not empty or None
                for result in search_results:
                    result_url = result.get('url', '').strip()
                    normalized_result_url = re.sub(r'^https?://(www\.)?', '', result_url).lower()
                    
                    if normalized_client_site in normalized_result_url:
                        client_rank = result.get('rank')
                    else:
                        for comp in normalized_competitors:
                            if comp['normalized_url'] in normalized_result_url:
                                # Store rank if it's better than current recorded rank for this competitor
                                if comp['name'] not in competitor_ranks_map or result.get('rank') < competitor_ranks_map[comp['name']]['rank']:
                                    competitor_ranks_map[comp['name']] = {
                                        'name': comp.get('name'),
                                        'url': comp.get('url'),
                                        'rank': result.get('rank')
                                    }
                
                # If we got results, break the retry loop
                break
            else:
                attempt += 1
                print(f"Attempt {attempt} failed for keyword '{keyword}'. No results returned. Retrying...")

        except Exception as e:
            attempt += 1
            print(f"Error during scrape for keyword '{keyword}' on attempt {attempt}: {e}. Retrying...")
            if attempt == max_retries:
                print(f"Max retries reached for keyword '{keyword}'.")
                # Return nulls if all retries fail
                return {
                    'keyword': keyword,
                    'client_rank': None,
                    'competitors': [{'name': comp.get('name'), 'url': comp.get('url'), 'rank': None} for comp in competitors]
                }

    # Ensure all competitors are in the output, even if they didn't rank
    final_competitor_ranks_list = []
    for comp in normalized_competitors:
        if comp['name'] in competitor_ranks_map:
            final_competitor_ranks_list.append(competitor_ranks_map[comp['name']])
        else:
            final_competitor_ranks_list.append({
                'name': comp.get('name'),
                'url': comp.get('url'),
                'rank': None
            })

    return {
        'keyword': keyword,
        'client_rank': client_rank,
        'competitors': final_competitor_ranks_list
    }

def aggregate_results(contact, keywords_analysis_results):
    """
    Aggregates keyword analysis results into a Competitor Matrix JSON object
    and derives insights.

    Args:
        contact (dict): Dictionary containing client information.
                        Expected keys: 'post_id', 'website_url'.
        keywords_analysis_results (list): A list of dictionaries, where each dictionary
                                          is the output of analyze_keyword for a specific keyword.

    Returns:
        dict: The Competitor Matrix JSON object.
    """
    contact_id = contact.get('post_id', 'unknown_id')
    client_site = contact.get('website_url', '')
    
    competitor_matrix = {
        "contact_id": contact_id,
        "client_site": client_site,
        "keywords_tracked": [],
        "competitor_summary": [] # This part is not implemented in this function, as it requires more detailed analysis than just ranking.
                                  # It would typically involve analyzing competitor content, backlinks, etc.
                                  # For now, we'll leave it empty or with a placeholder.
    }

    arbitrage_opportunities = []
    client_strengths = []
    open_markets = []

    for analysis in keywords_analysis_results:
        keyword = analysis.get('keyword')
        client_rank = analysis.get('client_rank')
        competitors_data = analysis.get('competitors', [])

        # Add to keywords_tracked
        competitor_ranks_for_keyword = []
        for comp_data in competitors_data:
            competitor_ranks_for_keyword.append({
                "name": comp_data.get('name'),
                "url": comp_data.get('url'),
                "rank": comp_data.get('rank') # Corrected: should be comp_data.get('rank')
            })
        
        competitor_matrix["keywords_tracked"].append({
            "keyword": keyword,
            "client_rank": client_rank,
            "top_competitors": competitor_ranks_for_keyword
        })

        # Determine insights
        competitor_ranks = [comp.get('rank') for comp in competitors_data if comp.get('rank') is not None]
        min_competitor_rank = min(competitor_ranks) if competitor_ranks else None
        
        # Arbitrage Opportunity: Client absent/low rank, competitor high rank
        if (client_rank is None or client_rank > 10) and min_competitor_rank is not None and min_competitor_rank <= 10:
            arbitrage_opportunities.append({
                "keyword": keyword,
                "note": "Client missing/low rank, competitor ranks high"
            })
        
        # Client Strength: Client ranks well, outranks competitors
        elif client_rank is not None and client_rank <= 10 and (min_competitor_rank is None or client_rank < min_competitor_rank):
            client_strengths.append({
                "keyword": keyword,
                "note": "Client outranks competitors"
            })
        
        # Open Market: Client and all competitors are absent from top 10
        elif client_rank is None and all(comp.get('rank') is None for comp in competitors_data):
            open_markets.append({
                "keyword": keyword,
                "note": "No major competitor presence"
            })

    competitor_matrix["insights"] = {
        "arbitrage_opportunities": arbitrage_opportunities,
        "client_strengths": client_strengths,
        "open_markets": open_markets
    }

    return competitor_matrix

def run_seo_analysis(contact_data, competitors_data):
    """
    Orchestrates the SEO Competitor Analysis Agent workflow.

    Args:
        contact_data (dict): Client contact information.
        competitors_data (list): List of competitor dictionaries.

    Returns:
        str: A JSON string representing the Competitor Matrix.
    """
    print("Starting SEO Analysis...")
    
    # Phase 1: Generate Keywords
    generated_keywords = generate_keywords(contact_data, competitors_data)
    print(f"Generated Keywords: {generated_keywords}")

    # Phase 2: Analyze each keyword
    all_analysis_results = []
    for kw in generated_keywords:
        print(f"Analyzing keyword: '{kw}'...")
        analysis = analyze_keyword(kw, contact_data['website_url'], competitors_data)
        all_analysis_results.append(analysis)
        # Basic error handling check: if analyze_keyword returned None for all competitors, it might indicate a failure.
        # More robust error handling (like the 4-failure rule) would be implemented here.
        if analysis.get('client_rank') is None and all(comp.get('rank') is None for comp in analysis.get('competitors', [])):
            print(f"Warning: No results found for keyword '{kw}'.")

    # Phase 3: Aggregate results into Competitor Matrix JSON
    print("Aggregating results...")
    final_matrix = aggregate_results(contact_data, all_analysis_results)
    
    print("SEO Analysis Complete.")
    return json.dumps(final_matrix, indent=2)

# Example Usage (for demonstration purposes)
if __name__ == "__main__":
    # Sample Input Data
    contact_data = {
      "post_id": "12345", # Changed to match example JSON
      "business_name": "PRO WELDING FABRICATORS, LLC",
      "category": "Welding Services",
      "services_rendered": [
        "Onsite Mobile Welding",
        "Staircase fabrication and repair",
        "Gate fabrication and repair",
        "Railing fabrication and repair",
        "Fence fabrication and repair",
        "Patio work",
        "Rust Corrosion Repair"
      ],
      "name": "Gio Pugliese",
      "email": None,
      "phone": "689-777-1709",
      "website_url": "pwfflorida.com",
      "City": "Orlando,Florida"
    }

    competitors_data = [
      { "name": "Tony's Welding, LLC", "url": "tonywelding.com" },
      { "name": "Orlando Fab Works", "url": "orlandofab.com" },
      { "name": "Superior Gates", "url": "superiorgates.com" } # Added another competitor for better testing
    ]

    # Run the analysis
    competitor_matrix_json = run_seo_analysis(contact_data, competitors_data)
    
    # Print the final JSON output
    print("\n--- Competitor Matrix JSON ---")
    print(competitor_matrix_json)
    print("----------------------------")

</final_file_content>

IMPORTANT: For any future changes to this file, use the final_file_content shown above as your reference. This content reflects the current state of the file, including any auto-formatting (e.g., if you used single quotes but the formatter converted them to double quotes). Always base your SEARCH/REPLACE operations on this final version to ensure accuracy.

<environment_details>
# VSCode Visible Files
seo_agent.py

# VSCode Open Tabs
category_analyzer.py
get_db_schema.py
config.json
google_maps_scraper.py
seo_agent.py
email-sender.py

# Current Time
8/18/2025, 2:31:35 PM (America/Bogota, UTC-5:00)

# Context Window Usage
179,179 / 1,000K tokens used (18%)

# Current Mode
ACT MODE
</environment_details>
