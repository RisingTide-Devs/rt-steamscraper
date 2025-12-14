"""
Database Manager for Steam Scraper
Handles all PostgreSQL database operations.
"""

import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from typing import Dict, Optional


class DatabaseManager:
    def __init__(self, config):
        """
        Initialize DatabaseManager with connection config.
        
        Args:
            config: Either a connection URL string or a dict with connection parameters
        """
        self.config = config
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to PostgreSQL database."""
        if isinstance(self.config, str):
            # DB_URL connection string
            self.conn = psycopg2.connect(self.config)
        else:
            # Dict-based config
            self.conn = psycopg2.connect(**self.config)
        self.cursor = self.conn.cursor()
        print("Database connection established")
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed")
    
    def get_existing_app_ids(self) -> set:
        """Get set of all existing steam_app_id values in database."""
        try:
            self.cursor.execute("SELECT steam_app_id FROM steam_products WHERE steam_app_id IS NOT NULL")
            return {row[0] for row in self.cursor.fetchall()}
        except Exception as e:
            print(f"Error fetching existing app IDs: {e}")
            return set()
    
    def app_exists(self, app_id: int) -> bool:
        """Check if app already exists in database."""
        try:
            self.cursor.execute(
                "SELECT 1 FROM steam_products WHERE steam_app_id = %s LIMIT 1",
                (app_id,)
            )
            return self.cursor.fetchone() is not None
        except Exception as e:
            print(f"Error checking if app exists: {e}")
            return False
    
    def insert_steam_product(self, app_data: Dict) -> tuple[bool, str]:
        """
        Insert or update steam product data.
        
        Args:
            app_data: Dictionary containing Steam app details
            
        Returns:
            (success: bool, action: str) where action is 'inserted', 'updated', or 'failed'
        """
        try:
            # Extract platform support
            platforms = app_data.get('platforms', {})
            
            # Extract pricing info
            price_overview = app_data.get('price_overview', {})
            is_free = app_data.get('is_free', False)
            
            # Extract release date
            release_date = app_data.get('release_date', {})
            
            # Extract requirements
            pc_requirements = app_data.get('pc_requirements', {})
            
            # Check if app exists before inserting
            app_id = app_data.get('steam_appid')
            existed = self.app_exists(app_id)
            
            query = """
            INSERT INTO steam_products (
                steam_app_id, name, type, description, short_description,
                detailed_description, header_image, capsule_image,
                screenshots, movies, release_date, coming_soon,
                developers, publishers, is_free, price, currency,
                discount_percent, original_price, supports_windows,
                supports_mac, supports_linux, minimum_requirements,
                recommended_requirements, categories, genres,
                supported_languages, website, legal_notice,
                required_age, content_descriptors, controller_support,
                created_at, updated_at, last_scraped
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON CONFLICT (steam_app_id) 
            DO UPDATE SET
                name = EXCLUDED.name,
                type = EXCLUDED.type,
                description = EXCLUDED.description,
                short_description = EXCLUDED.short_description,
                detailed_description = EXCLUDED.detailed_description,
                header_image = EXCLUDED.header_image,
                capsule_image = EXCLUDED.capsule_image,
                screenshots = EXCLUDED.screenshots,
                movies = EXCLUDED.movies,
                release_date = EXCLUDED.release_date,
                coming_soon = EXCLUDED.coming_soon,
                developers = EXCLUDED.developers,
                publishers = EXCLUDED.publishers,
                is_free = EXCLUDED.is_free,
                price = EXCLUDED.price,
                currency = EXCLUDED.currency,
                discount_percent = EXCLUDED.discount_percent,
                original_price = EXCLUDED.original_price,
                supports_windows = EXCLUDED.supports_windows,
                supports_mac = EXCLUDED.supports_mac,
                supports_linux = EXCLUDED.supports_linux,
                minimum_requirements = EXCLUDED.minimum_requirements,
                recommended_requirements = EXCLUDED.recommended_requirements,
                categories = EXCLUDED.categories,
                genres = EXCLUDED.genres,
                supported_languages = EXCLUDED.supported_languages,
                website = EXCLUDED.website,
                legal_notice = EXCLUDED.legal_notice,
                required_age = EXCLUDED.required_age,
                content_descriptors = EXCLUDED.content_descriptors,
                controller_support = EXCLUDED.controller_support,
                updated_at = EXCLUDED.updated_at,
                last_scraped = EXCLUDED.last_scraped
            WHERE steam_products.steam_app_id = EXCLUDED.steam_app_id
            """
            
            now = datetime.now()
            
            values = (
                app_id,
                app_data.get('name'),
                app_data.get('type'),
                app_data.get('about_the_game'),
                app_data.get('short_description'),
                app_data.get('detailed_description'),
                app_data.get('header_image'),
                app_data.get('capsule_image'),
                Json([s.get('path_full') for s in app_data.get('screenshots', [])]),
                Json(app_data.get('movies', [])),
                release_date.get('date'),
                release_date.get('coming_soon', False),
                Json(app_data.get('developers', [])),
                Json(app_data.get('publishers', [])),
                is_free,
                price_overview.get('final') / 100.0 if price_overview else None,
                price_overview.get('currency') if price_overview else None,
                price_overview.get('discount_percent') if price_overview else 0,
                price_overview.get('initial') / 100.0 if price_overview else None,
                platforms.get('windows', False),
                platforms.get('mac', False),
                platforms.get('linux', False),
                Json(pc_requirements.get('minimum', {})) if pc_requirements else None,
                Json(pc_requirements.get('recommended', {})) if pc_requirements else None,
                Json(app_data.get('categories', [])),
                Json(app_data.get('genres', [])),
                Json(app_data.get('supported_languages')),
                app_data.get('website'),
                app_data.get('legal_notice'),
                app_data.get('required_age', 0),
                Json(app_data.get('content_descriptors', {})),
                app_data.get('controller_support'),
                now,  # created_at - only used on INSERT
                now,  # updated_at
                now   # last_scraped
            )
            
            self.cursor.execute(query, values)
            self.conn.commit()
            
            action = 'updated' if existed else 'inserted'
            return True, action
            
        except Exception as e:
            print(f"Error inserting/updating product {app_data.get('steam_appid')}: {e}")
            self.conn.rollback()
            return False, 'failed'