"""
Steam Web API Scraper
Scrapes product data from Steam Web API and stores it in PostgreSQL database.
"""

import os
import time
import requests
from datetime import datetime
from typing import Dict, Optional, List
from dotenv import load_dotenv
from database_manager import DatabaseManager

# Load environment variables from .env file
load_dotenv()

# Configuration
STEAM_API_KEY = os.getenv('STEAM_API_KEY', 'YOUR_API_KEY_HERE')

# Database connection - use DB_URL if available, otherwise fall back to individual params
DB_URL = os.getenv('DB_URL')
if DB_URL:
    DB_CONFIG = DB_URL
else:
    # Fallback to individual parameters
    DB_CONFIG = {
        'dbname': 'BI-Platform',
        'user': os.getenv('USERNAME', 'rtdevs'),
        'password': os.getenv('PASSWORD', ''),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }

# Rate limiting
DELAY_BETWEEN_REQUESTS = 1 # seconds between requests


class SteamScraper:
    def __init__(self):
        self.session = requests.Session()
        
    def _rate_limit(self):
        """Enforce rate limiting between API requests."""
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    def get_app_details(self, app_id: int) -> Optional[Dict]:
        """Fetch app details from Steam Store API."""
        self._rate_limit()
        
        url = f"https://store.steampowered.com/api/appdetails"
        params = {'appids': app_id}
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if str(app_id) in data and data[str(app_id)]['success']:
                return data[str(app_id)]['data']
            return None
            
        except Exception as e:
            print(f"Error fetching app {app_id}: {e}")
            return None
    
    def get_app_list(self) -> List[Dict]:
        """Fetch list of all Steam apps in batches."""
        url = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
        all_apps = []
        last_appid = 0
        batch_size = 50000
        
        print("Fetching apps in batches...")
        
        while True:
            time.sleep(DELAY_BETWEEN_REQUESTS)
            
            params = {
                'key': STEAM_API_KEY,
                'max_results': batch_size,
                'last_appid': last_appid,
                'include_dlc': True,
                'include_software': True
            }
            
            try:
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                apps = data.get('response', {}).get('apps', [])
                have_more = data.get('response', {}).get('have_more_results', False)
                
                if not apps:
                    # No more apps to fetch
                    print(f"  No more apps returned. API response: {data.get('response', {})}")
                    break
                
                all_apps.extend(apps)
                last_appid = apps[-1]['appid']
                
                print(f"  Fetched {len(apps)} apps (total: {len(all_apps)}, last ID: {last_appid}, have_more: {have_more})")
                
                # Check if there are more results
                if not have_more:
                    print(f"  API indicates no more results. Final response keys: {data.get('response', {}).keys()}")
                    break
                    
            except Exception as e:
                print(f"Error fetching app list batch: {e}")
                break
        
        return all_apps


def main():
    """Main scraping function."""
    print("Starting Steam Web API Scraper...")
    
    # Initialize scraper and database
    scraper = SteamScraper()
    db = DatabaseManager(DB_CONFIG)
    
    try:
        # Connect to database
        db.connect()
        
        # Get existing app IDs
        print("Checking existing records...")
        existing_app_ids = db.get_existing_app_ids()
        print(f"Found {len(existing_app_ids)} existing products in database")
        
        # Get list of all Steam apps
        print("\nFetching app list from Steam...")
        apps = scraper.get_app_list()
        print(f"Found {len(apps)} apps on Steam")
        
        # Scrape details for each app
        apps_to_scrape = apps  # Scrape all apps
        
        # Count stats
        insert_count = 0
        update_count = 0
        skip_count = 0
        fail_count = 0
        
        print(f"\nStarting to scrape {len(apps_to_scrape)} apps...\n")
        
        for i, app in enumerate(apps_to_scrape, 1):
            app_id = app['appid']
            app_name = app['name']
            
            status = "EXISTS" if app_id in existing_app_ids else "NEW"
            print(f"[{i}/{len(apps_to_scrape)}] [{status}] {app_name} (ID: {app_id})...")
            
            # Fetch app details
            app_data = scraper.get_app_details(app_id)
            
            if app_data:
                # Store in database
                success, action = db.insert_steam_product(app_data)
                
                if success:
                    if action == 'inserted':
                        insert_count += 1
                        print(f"  ✓ Inserted new product")
                    elif action == 'updated':
                        update_count += 1
                        print(f"  ✓ Updated existing product")
                else:
                    fail_count += 1
                    print(f"  ✗ Failed to store")
            else:
                skip_count += 1
                print(f"  ✗ No data available")
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"SCRAPING COMPLETE")
        print(f"{'='*60}")
        print(f"Total apps processed: {len(apps_to_scrape)}")
        print(f"New products inserted: {insert_count}")
        print(f"Existing products updated: {update_count}")
        print(f"Skipped (no data): {skip_count}")
        print(f"Failed: {fail_count}")
        print(f"{'='*60}")
        
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.disconnect()


if __name__ == "__main__":
    main()