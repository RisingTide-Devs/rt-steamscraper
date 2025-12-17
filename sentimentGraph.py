#!/usr/bin/env python3
"""
Sentiment Analysis Visualization - Per Game
Creates individual graphs for each game's sentiment analysis
"""

import os
import sys
import argparse
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import numpy as np

# Load environment variables
load_dotenv()

# Import database manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database_manager import DatabaseManager


def create_game_sentiment_overview(db, app_id, game_name):
    """Create comprehensive sentiment overview for a single game"""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(f'Sentiment Analysis: {game_name}', fontsize=16, fontweight='bold')
    
    # 1. Overall Sentiment Distribution (Pie Chart)
    query1 = """
    SELECT 
        SUM(asu.positive_count) as positive,
        SUM(asu.negative_count) as negative,
        SUM(asu.neutral_count) as neutral
    FROM sentiment_analysis sa
    JOIN analysis_summaries asu ON asu.sentiment_analysis_id = sa.id
    WHERE sa.from_game = %s
    """
    db.cursor.execute(query1, (game_name,))
    result = db.cursor.fetchone()
    
    if result and sum(result) > 0:
        positive, negative, neutral = result
        sizes = [positive, negative, neutral]
        labels = [f'Positive ({positive})', f'Negative ({negative})', f'Neutral ({neutral})']
        colors = ['green', 'red', 'gray']
        
        ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%',
               shadow=True, startangle=90, textprops={'fontsize': 10, 'fontweight': 'bold'})
        ax1.set_title('Overall Sentiment Distribution', fontsize=12, fontweight='bold')
    
    # 2. Top 10 Categories (Horizontal Bar)
    query2 = """
    SELECT 
        ac.category,
        COUNT(*) as chunk_count,
        SUM(CASE WHEN ac.sentiment = 'positive' THEN 1 ELSE 0 END) as positive,
        SUM(CASE WHEN ac.sentiment = 'negative' THEN 1 ELSE 0 END) as negative,
        SUM(CASE WHEN ac.sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral
    FROM analysis_chunks ac
    JOIN sentiment_analysis sa ON sa.id = ac.sentiment_analysis_id
    WHERE sa.from_game = %s
    GROUP BY ac.category
    ORDER BY chunk_count DESC
    LIMIT 10
    """
    db.cursor.execute(query2, (game_name,))
    categories = db.cursor.fetchall()
    
    if categories:
        cat_names = [c[0] for c in categories]
        positive = [c[2] for c in categories]
        negative = [c[3] for c in categories]
        neutral = [c[4] for c in categories]
        
        y = np.arange(len(cat_names))
        width = 0.25
        
        ax2.barh(y - width, positive, width, label='Positive', color='green', alpha=0.8)
        ax2.barh(y, negative, width, label='Negative', color='red', alpha=0.8)
        ax2.barh(y + width, neutral, width, label='Neutral', color='gray', alpha=0.8)
        
        ax2.set_yticks(y)
        ax2.set_yticklabels(cat_names, fontsize=9)
        ax2.set_xlabel('Number of Mentions', fontsize=10, fontweight='bold')
        ax2.set_title('Top 10 Categories by Sentiment', fontsize=12, fontweight='bold')
        ax2.legend(fontsize=9)
        ax2.grid(axis='x', alpha=0.3)
    
    # 3. Confidence Score Distribution (Histogram)
    query3 = """
    SELECT confidence
    FROM analysis_chunks ac
    JOIN sentiment_analysis sa ON sa.id = ac.sentiment_analysis_id
    WHERE sa.from_game = %s
    """
    db.cursor.execute(query3, (game_name,))
    confidences = [row[0] for row in db.cursor.fetchall()]
    
    if confidences:
        ax3.hist(confidences, bins=15, color='steelblue', alpha=0.7, edgecolor='black')
        ax3.axvline(np.mean(confidences), color='red', linestyle='--', linewidth=2,
                   label=f'Mean: {np.mean(confidences):.2f}')
        ax3.set_xlabel('Confidence Score', fontsize=10, fontweight='bold')
        ax3.set_ylabel('Frequency', fontsize=10, fontweight='bold')
        ax3.set_title('Confidence Score Distribution', fontsize=12, fontweight='bold')
        ax3.legend(fontsize=9)
        ax3.grid(axis='y', alpha=0.3)
    
    # 4. Sentiment by Category (Net Sentiment)
    if categories:
        cat_names_net = [c[0] for c in categories]
        net_sentiment = [c[2] - c[3] for c in categories]  # positive - negative
        colors_net = ['green' if x > 0 else 'red' if x < 0 else 'gray' for x in net_sentiment]
        
        y_net = np.arange(len(cat_names_net))
        ax4.barh(y_net, net_sentiment, color=colors_net, alpha=0.8, edgecolor='black')
        ax4.axvline(0, color='black', linewidth=1)
        ax4.set_yticks(y_net)
        ax4.set_yticklabels(cat_names_net, fontsize=9)
        ax4.set_xlabel('Net Sentiment (Positive - Negative)', fontsize=10, fontweight='bold')
        ax4.set_title('Net Sentiment by Category', fontsize=12, fontweight='bold')
        ax4.grid(axis='x', alpha=0.3)
    
    plt.tight_layout()
    return fig


def create_game_category_detail(db, app_id, game_name):
    """Create detailed category analysis for a single game"""
    query = """
    SELECT 
        ac.category,
        COUNT(*) as total_chunks,
        AVG(ac.confidence) as avg_confidence,
        SUM(CASE WHEN ac.sentiment = 'positive' THEN 1 ELSE 0 END) as positive,
        SUM(CASE WHEN ac.sentiment = 'negative' THEN 1 ELSE 0 END) as negative,
        SUM(CASE WHEN ac.sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral
    FROM analysis_chunks ac
    JOIN sentiment_analysis sa ON sa.id = ac.sentiment_analysis_id
    WHERE sa.from_game = %s
    GROUP BY ac.category
    ORDER BY total_chunks DESC
    LIMIT 15
    """
    db.cursor.execute(query, (game_name,))
    results = db.cursor.fetchall()
    
    if not results:
        return None
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    fig.suptitle(f'Category Analysis: {game_name}', fontsize=16, fontweight='bold')
    
    # 1. Stacked bar chart showing sentiment breakdown
    categories = [r[0] for r in results]
    positive = [r[3] for r in results]
    negative = [r[4] for r in results]
    neutral = [r[5] for r in results]
    
    y = np.arange(len(categories))
    
    ax1.barh(y, positive, label='Positive', color='green', alpha=0.8)
    ax1.barh(y, negative, left=positive, label='Negative', color='red', alpha=0.8)
    ax1.barh(y, neutral, left=np.array(positive) + np.array(negative), 
            label='Neutral', color='gray', alpha=0.8)
    
    ax1.set_yticks(y)
    ax1.set_yticklabels(categories, fontsize=10)
    ax1.set_xlabel('Number of Mentions', fontsize=11, fontweight='bold')
    ax1.set_title('Category Mentions by Sentiment', fontsize=13, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(axis='x', alpha=0.3)
    
    # 2. Average confidence per category
    confidences = [r[2] for r in results]
    
    bars = ax2.barh(y, confidences, color='steelblue', alpha=0.8, edgecolor='darkblue', linewidth=1.5)
    ax2.axvline(np.mean(confidences), color='red', linestyle='--', linewidth=2,
               label=f'Avg: {np.mean(confidences):.2f}')
    ax2.set_yticks(y)
    ax2.set_yticklabels(categories, fontsize=10)
    ax2.set_xlabel('Average Confidence', fontsize=11, fontweight='bold')
    ax2.set_title('Average Confidence by Category', fontsize=13, fontweight='bold')
    ax2.set_xlim(0, 1.0)
    ax2.legend(fontsize=10)
    ax2.grid(axis='x', alpha=0.3)
    
    # Add value labels on bars
    for bar, conf in zip(bars, confidences):
        width = bar.get_width()
        ax2.text(width + 0.02, bar.get_y() + bar.get_height()/2,
                f'{conf:.2f}', ha='left', va='center', fontsize=8, fontweight='bold')
    
    plt.tight_layout()
    return fig


def main():
    """Generate per-game sentiment visualizations"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate sentiment analysis visualizations per game')
    parser.add_argument('output_folder', nargs='?', default='.', 
                       help='Output folder for generated images (default: current directory)')
    args = parser.parse_args()
    
    # Create output folder if it doesn't exist
    output_folder = args.output_folder
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"📁 Created output folder: {output_folder}")
    
    # Get app IDs
    try:
        with open('app_ids.txt', 'r') as f:
            app_ids = [int(line.strip()) for line in f if line.strip()]
    except FileNotFoundError:
        print("Error: app_ids.txt not found")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"SENTIMENT ANALYSIS VISUALIZATION - PER GAME")
    print(f"{'='*80}")
    print(f"Creating graphs for {len(app_ids)} games: {app_ids}")
    print(f"Output folder: {os.path.abspath(output_folder)}")
    
    # Connect to database
    DB_URL = os.getenv('DB_URL')
    if not DB_URL:
        print("Error: DB_URL not found in .env file")
        sys.exit(1)
    
    db = DatabaseManager(DB_URL)
    db.connect()
    
    # Process each game
    for app_id in app_ids:
        # Get game name
        query = "SELECT name FROM steam_products WHERE steam_app_id = %s"
        db.cursor.execute(query, (app_id,))
        result = db.cursor.fetchone()
        
        if not result:
            print(f"\n⚠️  Game {app_id} not found in database, skipping...")
            continue
        
        game_name = result[0]
        safe_name = game_name.replace('/', '-').replace(' ', '_').replace(':', '')
        
        print(f"\n{'='*80}")
        print(f"Processing: {game_name} (ID: {app_id})")
        print(f"{'='*80}")
        
        # Check if sentiment data exists
        check_query = """
        SELECT COUNT(*) FROM sentiment_analysis WHERE from_game = %s
        """
        db.cursor.execute(check_query, (game_name,))
        count = db.cursor.fetchone()[0]
        
        if count == 0:
            print(f"  ⚠️  No sentiment analysis data found for {game_name}, skipping...")
            continue
        
        print(f"  📊 Found {count} sentiment analyses")
        
        # 1. Create overview dashboard
        print(f"  📊 Creating sentiment overview...")
        fig1 = create_game_sentiment_overview(db, app_id, game_name)
        filename1 = os.path.join(output_folder, f'sentiment_overview_{safe_name}.png')
        fig1.savefig(filename1, dpi=300, bbox_inches='tight')
        print(f"     ✅ Saved: {filename1}")
        plt.close(fig1)
        
        # 2. Create category detail
        print(f"  📊 Creating category analysis...")
        fig2 = create_game_category_detail(db, app_id, game_name)
        if fig2:
            filename2 = os.path.join(output_folder, f'sentiment_categories_{safe_name}.png')
            fig2.savefig(filename2, dpi=300, bbox_inches='tight')
            print(f"     ✅ Saved: {filename2}")
            plt.close(fig2)
    
    db.disconnect()
    
    print(f"\n{'='*80}")
    print(f"Visualization complete!")
    print(f"Output folder: {os.path.abspath(output_folder)}")
    print(f"Generated 2 graphs per game:")
    print(f"  - sentiment_overview_[GameName].png (4-panel dashboard)")
    print(f"  - sentiment_categories_[GameName].png (detailed category analysis)")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()