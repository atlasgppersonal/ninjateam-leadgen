import csv
import os
import json

def get_serpstat_urls(csv_file_path):
    urls = []
    base_url = "https://serpstat.com" # This base URL might need adjustment if the browser reveals a different one
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            if len(row) == 2:
                method_name = row[0]
                relative_url = row[1]
                full_url = f"{base_url}{relative_url}"
                urls.append({"method": method_name, "url": full_url})
    return urls

if __name__ == "__main__":
    csv_path = "c:/Users/GIO PLUGLIESE/Downloads/serpstat_methods.csv"
    serpstat_api_urls = get_serpstat_urls(csv_path)

    # Process only the first URL for demonstration and testing
    if serpstat_api_urls:
        first_entry = serpstat_api_urls[0]
        print(f"<browser_action>\n<action>launch</action>\n<url>{first_entry['url']}</url>\n</browser_action>")
