#!/usr/bin/env python3
"""
Steam Review Scraper with Database Integration
Scrapes reviews and uses Steam API to fill in missing data
No artificial limits - scrapes all available reviews
"""

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import re
import requests
import time
from datetime import datetime
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import the database manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database_manager import DatabaseManager

def get_numeric_steam_id(custom_id):
    """Convert custom Steam ID to numeric Steam ID using Steam API"""
    try:
        xml_url = f"https://steamcommunity.com/id/{custom_id}/?xml=1"
        response = requests.get(xml_url, timeout=5)
        
        if response.status_code == 200:
            match = re.search(r'<steamID64>(\d+)</steamID64>', response.text)
            if match:
                return match.group(1)
        return None
    except:
        return None

def parse_date(date_str):
    """Parse Steam review date format to datetime"""
    try:
        date_str = date_str.replace("Posted: ", "").strip()
        return datetime.strptime(date_str, "%B %d, %Y")
    except:
        return None

def parse_hours(hours_str):
    """Extract playtime in minutes from hours string"""
    try:
        match = re.search(r'([\d,]+\.?\d*)\s*hrs?', hours_str)
        if match:
            hours = float(match.group(1).replace(',', ''))
            return int(hours * 60)  # Convert to minutes
        return 0
    except:
        return 0

# Configuration
app_id = 1901370  # Ib game
steam_app_id = app_id
url = f"https://steamcommunity.com/app/{app_id}/reviews/?browsefilter=toprated&snr=1_5_100010_"

# Get database URL from environment
DB_URL = os.getenv('DB_URL')
if not DB_URL:
    print("Error: DB_URL not found in .env file")
    sys.exit(1)

print(f"Fetching reviews from: {url}")
print(f"Target app_id: {steam_app_id}")

with sync_playwright() as p:
    # Launch browser
    print("Launching browser...")
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    
    # Go to the page
    page.goto(url, wait_until='networkidle')
    
    # Wait for reviews to load
    print("Waiting for reviews to load...")
    page.wait_for_selector('.apphub_Card', timeout=10000)
    
    # Click "See More Content" button until there are no more reviews
    print("Loading ALL reviews (this may take a long time)...")
    clicks = 0
    
    while True:
        try:
            button = page.locator('text=See More Content')
            if button.count() > 0 and button.is_visible():
                clicks += 1
                if clicks % 10 == 0:
                    print(f"  Clicked {clicks} times...")
                button.click()
                page.wait_for_timeout(1000)  # Rate limit: 1 second between clicks
            else:
                print(f"  No more content button found after {clicks} clicks")
                break
        except Exception as e:
            print(f"  Stopped at {clicks} clicks - {e}")
            break
    
    print(f"Total clicks: {clicks}")
    
    # Get page content
    html = page.content()
    
    # Close browser
    browser.close()

print("Parsing reviews...")
soup = BeautifulSoup(html, 'html.parser')

# Find all review cards
review_cards = soup.find_all('div', class_='apphub_Card')

all_reviews = []
custom_id_count = 0

for idx, card in enumerate(review_cards, 1):
    card_text = card.get_text()
    
    # Check if it's a review
    if 'Recommended' not in card_text:
        continue
    
    # Extract username
    username_elem = card.find('div', class_='apphub_CardContentAuthorName')
    username = username_elem.get_text().strip() if username_elem else "Unknown"
    
    # Extract Steam ID from profile link
    steam_id = None
    author_link = card.find('a', href=re.compile(r'steamcommunity\.com/(profiles|id)/'))
    if author_link:
        href = author_link['href']
        profile_match = re.search(r'/profiles/(\d+)', href)
        if profile_match:
            steam_id = profile_match.group(1)
        else:
            id_match = re.search(r'/id/([^/]+)', href)
            if id_match:
                custom_id = id_match.group(1)
                custom_id_count += 1
                if custom_id_count % 10 == 1:
                    print(f"  Converting custom IDs to numeric... ({custom_id_count})")
                numeric_id = get_numeric_steam_id(custom_id)
                if numeric_id:
                    steam_id = numeric_id
                time.sleep(0.1)  # Rate limit: 0.1 seconds between conversions
    
    # Skip if we couldn't get a steam ID
    if not steam_id:
        continue
    
    # Extract basic data from web scraping
    voted_up = "Not Recommended" not in card_text
    
    hours_elem = card.find('div', class_='hours')
    playtime_forever = parse_hours(hours_elem.get_text().strip()) if hours_elem else 0
    
    review_text_elem = card.find('div', class_='apphub_CardTextContent')
    review_text = review_text_elem.get_text().strip() if review_text_elem else ""
    
    date_elem = card.find('div', class_='date_posted')
    date_str = date_elem.get_text().strip() if date_elem else ""
    timestamp_created = parse_date(date_str)
    
    all_reviews.append({
        'steam_id': steam_id,
        'username': username,
        'voted_up': voted_up,
        'playtime_forever': playtime_forever,
        'review_text': review_text,
        'timestamp_created': timestamp_created
    })

print(f"Converted {custom_id_count} custom IDs to numeric")
print(f"Total reviews parsed: {len(all_reviews)}")

# Now fetch additional data from Steam API
print("\nFetching additional review data from Steam API...")
print("This will fetch ALL available reviews from the API...")

# Fetch all reviews for this app from API
api_reviews = {}
cursor = '*'
page_num = 0

while True:  # No page limit - fetch all available
    try:
        api_url = f"https://store.steampowered.com/appreviews/{app_id}"
        params = {
            'json': 1,
            'filter': 'recent',
            'language': 'all',
            'day_range': 9223372036854775807,
            'num_per_page': 100,
            'review_type': 'all',
            'purchase_type': 'all',
            'cursor': cursor
        }
        
        response = requests.get(api_url, params=params, timeout=10)
        if response.status_code != 200:
            print(f"  API returned status {response.status_code}, stopping")
            break
        
        data = response.json()
        
        if not data.get('success'):
            print("  API request unsuccessful, stopping")
            break
            
        reviews = data.get('reviews', [])
        if not reviews:
            print("  No more reviews available from API")
            break
        
        for review in reviews:
            author_steamid = review.get('author', {}).get('steamid')
            if author_steamid:
                api_reviews[author_steamid] = review
        
        print(f"  Fetched API page {page_num + 1}, total API reviews: {len(api_reviews)}")
        
        cursor = data.get('cursor')
        if not cursor or cursor == '*':
            print("  No more pages available")
            break
        
        page_num += 1
        time.sleep(1)  # Rate limit: 1 second between API requests
        
    except Exception as e:
        print(f"  Error fetching API reviews: {e}")
        break

print(f"Total reviews from API: {len(api_reviews)}")

# Merge API data with scraped data
final_reviews = []

for review in all_reviews:
    steam_id = review['steam_id']
    api_data = api_reviews.get(steam_id, {})
    
    # Extract API data if available
    author_data = api_data.get('author', {})
    
    review_data = {
        'steam_product_id': steam_app_id,
        'review_id': api_data.get('recommendationid', f"{steam_id}_{steam_app_id}"),
        'author_steamid': steam_id,
        'author_playtime_forever': author_data.get('playtime_forever', review['playtime_forever']),
        'author_playtime_last_two_weeks': author_data.get('playtime_last_two_weeks'),
        'author_num_games_owned': author_data.get('num_games_owned'),
        'author_num_reviews': author_data.get('num_reviews'),
        'language': api_data.get('language', 'english'),
        'review': api_data.get('review', review['review_text']),
        'timestamp_created': datetime.fromtimestamp(api_data['timestamp_created']) if api_data.get('timestamp_created') else review['timestamp_created'],
        'timestamp_updated': datetime.fromtimestamp(api_data['timestamp_updated']) if api_data.get('timestamp_updated') else None,
        'voted_up': api_data.get('voted_up', review['voted_up']),
        'votes_up': api_data.get('votes_up'),
        'votes_funny': api_data.get('votes_funny'),
        'weighted_vote_score': api_data.get('weighted_vote_score'),
        'comment_count': api_data.get('comment_count'),
        'steam_purchase': api_data.get('steam_purchase'),
        'received_for_free': api_data.get('received_for_free'),
        'written_during_early_access': api_data.get('written_during_early_access'),
        'created_at': datetime.now()
    }
    
    final_reviews.append(review_data)

# Save to database
print("\nConnecting to database...")
db = DatabaseManager(DB_URL)
db.connect()

print("Saving reviews to database...")
success_count = 0
failed_count = 0

for review in final_reviews:
    if db.save_review(review):
        success_count += 1
    else:
        failed_count += 1
    
    if (success_count + failed_count) % 50 == 0:
        print(f"  Saved {success_count}, Failed {failed_count}")

print(f"\nFinal results:")
print(f"  Successfully saved: {success_count}")
print(f"  Failed: {failed_count}")
print(f"  Reviews with API data: {sum(1 for r in final_reviews if r['votes_up'] is not None)}")

db.disconnect()
print("\nDone!")