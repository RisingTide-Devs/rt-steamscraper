#!/usr/bin/env python3
"""
Top Sentiments Analyzer using LLM
Uses local Ollama to extract common sentiments from reviews
"""

import os
import sys
from dotenv import load_dotenv
from collections import Counter
import matplotlib.pyplot as plt
import json
from openai import OpenAI

# Load environment variables
load_dotenv()

# Import database manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database_manager import DatabaseManager


def extract_sentiments_from_review(client, review_text):
    """Use LLM to extract 3-5 key sentiments from a review"""
    try:
        response = client.chat.completions.create(
            model="gemma3:4b",
            messages=[
                {
                    "role": "system",
                    "content": """You are a sentiment extraction expert. Extract 3-5 key sentiments from game reviews.
                    
                    Format each sentiment as a short phrase (2-4 words) like:
                    - "great music"
                    - "fun gameplay"
                    - "poor optimization"
                    - "engaging story"
                    - "repetitive mechanics"
                    
                    Respond with ONLY a JSON object:
                    {
                        "sentiments": ["sentiment1", "sentiment2", "sentiment3"]
                    }"""
                },
                {
                    "role": "user",
                    "content": f"Extract key sentiments from this review:\n\n{review_text}"
                }
            ],
            response_format={"type": "json_object"},
            temperature=0.3
        )
        
        result = json.loads(response.choices[0].message.content)
        return result.get("sentiments", [])
        
    except Exception as e:
        print(f"  ⚠️  Error extracting sentiments: {e}")
        return []


def main():
    """Extract and tally top sentiments from reviews"""
    
    # Get app IDs to analyze
    try:
        with open('app_ids.txt', 'r') as f:
            app_ids = [int(line.strip()) for line in f if line.strip()]
    except FileNotFoundError:
        print("Error: app_ids.txt not found")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"TOP SENTIMENTS ANALYZER (LLM-Powered)")
    print(f"{'='*80}")
    print(f"Analyzing sentiments for {len(app_ids)} games: {app_ids}")
    
    # Connect to database
    DB_URL = os.getenv('DB_URL')
    if not DB_URL:
        print("Error: DB_URL not found in .env file")
        sys.exit(1)
    
    db = DatabaseManager(DB_URL)
    db.connect()
    
    # Get review counts per title first
    print("\n" + "="*80)
    print("REVIEW COUNTS PER TITLE")
    print("="*80)
    
    placeholders = ','.join(['%s'] * len(app_ids))
    count_query = f"""
    SELECT 
        sr.steam_product_id,
        sp.name as game_name,
        COUNT(*) as total_reviews,
        SUM(CASE WHEN sr.review IS NOT NULL AND LENGTH(sr.review) > 50 THEN 1 ELSE 0 END) as valid_reviews
    FROM steam_reviews sr
    JOIN steam_products sp ON sp.steam_app_id = sr.steam_product_id
    WHERE sr.steam_product_id IN ({placeholders})
    GROUP BY sr.steam_product_id, sp.name
    ORDER BY total_reviews DESC
    """
    
    db.cursor.execute(count_query, tuple(app_ids))
    review_counts = db.cursor.fetchall()
    
    print(f"{'App ID':<10} {'Game Name':<40} {'Total':<10} {'Valid (>50 chars)':<20}")
    print("-"*80)
    
    total_all = 0
    total_valid = 0
    for app_id, game_name, total, valid in review_counts:
        print(f"{app_id:<10} {game_name:<40} {total:<10} {valid:<20}")
        total_all += total
        total_valid += valid
    
    print("-"*80)
    print(f"{'TOTAL':<10} {'':<40} {total_all:<10} {total_valid:<20}")
    print("="*80)
    
    # Setup Ollama client
    API_KEY = os.getenv('OPENAI_API_KEY', 'dummy-key')
    OLLAMA_URL = os.getenv('OLLAMA_URL', 'http://localhost:11434/v1')
    
    print(f"\nUsing Ollama at: {OLLAMA_URL}")
    print(f"Model: gemma3:4b")
    
    client = OpenAI(base_url=OLLAMA_URL, api_key=API_KEY)
    
    # Get reviews for these games
    query = f"""
    SELECT 
        sr.review,
        sp.name as game_name
    FROM steam_reviews sr
    JOIN steam_products sp ON sp.steam_app_id = sr.steam_product_id
    WHERE sr.steam_product_id IN ({placeholders})
    AND sr.review IS NOT NULL
    AND LENGTH(sr.review) > 50
    ORDER BY sr.steam_product_id, sr.id
    """
    
    db.cursor.execute(query, tuple(app_ids))
    reviews = db.cursor.fetchall()
    
    print(f"\n📊 Found {len(reviews)} valid reviews to analyze\n")
    
    # Extract sentiments
    all_sentiments = []
    game_sentiments = {}
    
    for idx, (review_text, game_name) in enumerate(reviews, 1):
        print(f"Processing review {idx}/{len(reviews)} ({game_name})...", end=' ')
        
        sentiments = extract_sentiments_from_review(client, review_text)
        
        if sentiments:
            all_sentiments.extend(sentiments)
            
            if game_name not in game_sentiments:
                game_sentiments[game_name] = []
            game_sentiments[game_name].extend(sentiments)
            
            print(f"✅ Extracted {len(sentiments)} sentiments")
        else:
            print("⚠️  No sentiments extracted")
        
        # Progress update every 10 reviews
        if idx % 10 == 0:
            print(f"  📈 Progress: {idx}/{len(reviews)} reviews processed")
    
    # Count sentiments
    sentiment_counts = Counter(all_sentiments)
    
    # Display overall top 5
    print("\n" + "="*80)
    print("TOP 5 MOST COMMON SENTIMENTS (All Games)")
    print("="*80)
    print(f"{'Rank':<6} {'Sentiment':<40} {'Count':<10}")
    print("-"*80)
    
    top_5_overall = sentiment_counts.most_common(5)
    for rank, (sentiment, count) in enumerate(top_5_overall, 1):
        print(f"{rank:<6} {sentiment:<40} {count:<10}")
    
    # Display per-game top 5
    print("\n" + "="*80)
    print("TOP 5 SENTIMENTS PER GAME")
    print("="*80)
    
    for game_name, sentiments in game_sentiments.items():
        game_counts = Counter(sentiments)
        top_5 = game_counts.most_common(5)
        
        print(f"\n{game_name}:")
        print(f"{'Rank':<6} {'Sentiment':<40} {'Count':<10}")
        print("-"*80)
        
        for rank, (sentiment, count) in enumerate(top_5, 1):
            print(f"{rank:<6} {sentiment:<40} {count:<10}")
    
    # Create visualization
    print("\n" + "="*80)
    print("Generating visualization...")
    print("="*80)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Plot 1: Top 10 overall sentiments
    top_10_overall = sentiment_counts.most_common(10)
    sentiments_list = [s for s, _ in top_10_overall]
    counts_list = [c for _, c in top_10_overall]
    
    bars = ax1.barh(sentiments_list, counts_list, color='steelblue', 
                    alpha=0.7, edgecolor='darkblue', linewidth=1.5)
    ax1.set_xlabel('Number of Mentions', fontsize=12, fontweight='bold')
    ax1.set_title('Top 10 Most Common Sentiments', fontsize=14, fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)
    ax1.invert_yaxis()
    
    # Add value labels
    for bar, count in zip(bars, counts_list):
        width = bar.get_width()
        ax1.text(width + max(counts_list)*0.01, bar.get_y() + bar.get_height()/2, 
                f'{int(count)}', ha='left', va='center', fontsize=9, fontweight='bold')
    
    # Plot 2: Top 5 per game comparison
    games = list(game_sentiments.keys())
    
    if len(games) <= 5:  # Only show if we have 5 or fewer games
        top_sentiments_per_game = {}
        for game, sentiments in game_sentiments.items():
            game_counts = Counter(sentiments)
            top_sentiments_per_game[game] = game_counts.most_common(5)
        
        # Get unique top sentiments across all games
        all_top_sentiments = set()
        for tops in top_sentiments_per_game.values():
            all_top_sentiments.update([s for s, _ in tops])
        
        # Limit to top 10 most common overall
        top_sentiments_to_show = [s for s, _ in sentiment_counts.most_common(10)]
        
        x = range(len(top_sentiments_to_show))
        width = 0.8 / len(games)
        
        for i, (game, tops) in enumerate(top_sentiments_per_game.items()):
            counts = []
            for sentiment in top_sentiments_to_show:
                # Find count for this sentiment in this game
                count = next((c for s, c in tops if s == sentiment), 0)
                counts.append(count)
            
            offset = (i - len(games)/2) * width + width/2
            ax2.bar([pos + offset for pos in x], counts, width, 
                   label=game[:20], alpha=0.7, edgecolor='black', linewidth=1)
        
        ax2.set_xlabel('Sentiment', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Number of Mentions', fontsize=12, fontweight='bold')
        ax2.set_title('Top Sentiments by Game', fontsize=14, fontweight='bold')
        ax2.set_xticks(x)
        ax2.set_xticklabels([s[:20] for s in top_sentiments_to_show], 
                            rotation=45, ha='right', fontsize=8)
        ax2.legend(fontsize=9)
        ax2.grid(axis='y', alpha=0.3)
    else:
        ax2.text(0.5, 0.5, f'Too many games ({len(games)}) to display comparison.\nSee per-game data in CSV.', 
                ha='center', va='center', fontsize=12, transform=ax2.transAxes)
        ax2.axis('off')
    
    plt.tight_layout()
    output_file = 'top_sentiments_analysis.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"💾 Visualization saved to: {output_file}")
    plt.close()
    
    # Export to CSV
    csv_file = 'top_sentiments_data.csv'
    with open(csv_file, 'w') as f:
        f.write("rank,sentiment,count\n")
        for rank, (sentiment, count) in enumerate(top_5_overall, 1):
            f.write(f'{rank},"{sentiment}",{count}\n')
    
    print(f"💾 Overall data saved to: {csv_file}")
    
    # Export per-game data
    game_csv_file = 'top_sentiments_per_game.csv'
    with open(game_csv_file, 'w') as f:
        f.write("game,rank,sentiment,count\n")
        for game_name, sentiments in game_sentiments.items():
            game_counts = Counter(sentiments)
            top_5 = game_counts.most_common(5)
            for rank, (sentiment, count) in enumerate(top_5, 1):
                f.write(f'"{game_name}",{rank},"{sentiment}",{count}\n')
    
    print(f"💾 Per-game data saved to: {game_csv_file}")
    
    # Export all sentiments with counts
    all_csv_file = 'all_sentiments_counts.csv'
    with open(all_csv_file, 'w') as f:
        f.write("sentiment,count\n")
        for sentiment, count in sentiment_counts.most_common():
            f.write(f'"{sentiment}",{count}\n')
    
    print(f"💾 All sentiments saved to: {all_csv_file}")
    
    db.disconnect()
    
    print(f"\n{'='*80}")
    print(f"Analysis complete!")
    print(f"  Total reviews analyzed: {len(reviews)}")
    print(f"  Total sentiments extracted: {len(all_sentiments)}")
    print(f"  Unique sentiments: {len(sentiment_counts)}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()