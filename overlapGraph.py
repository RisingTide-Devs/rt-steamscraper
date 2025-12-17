#!/usr/bin/env python3
"""
User Overlap Graph Generator
Creates a visualization showing user overlap between games
No networkx dependency - uses matplotlib only
"""

import os
import sys
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from itertools import combinations
import math

# Load environment variables
load_dotenv()

# Import database manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database_manager import DatabaseManager


def get_game_users(db, app_id):
    """Get all unique users who reviewed a game"""
    query = """
    SELECT DISTINCT author_steamid
    FROM steam_reviews
    WHERE steam_product_id = %s
    """
    db.cursor.execute(query, (app_id,))
    return set(row[0] for row in db.cursor.fetchall())


def get_game_name(db, app_id):
    """Get game name from app ID"""
    query = """
    SELECT name FROM steam_products WHERE steam_app_id = %s
    """
    db.cursor.execute(query, (app_id,))
    result = db.cursor.fetchone()
    return result[0] if result else str(app_id)


def calculate_overlap(users1, users2):
    """Calculate the number of overlapping users between two games"""
    return len(users1.intersection(users2))


def create_circular_layout(n):
    """Create circular layout positions for n nodes"""
    positions = {}
    for i in range(n):
        angle = 2 * math.pi * i / n
        x = math.cos(angle)
        y = math.sin(angle)
        positions[i] = (x, y)
    return positions


def get_label_offset(x1, y1, x2, y2, offset=0.15):
    """Calculate offset position for edge label to avoid center overlap"""
    # Get midpoint
    mid_x = (x1 + x2) / 2
    mid_y = (y1 + y2) / 2
    
    # Calculate distance from center
    dist = math.sqrt(mid_x**2 + mid_y**2)
    
    # If close to center, push outward
    if dist < 0.3:  # Very close to center
        # Push along the perpendicular to the line
        dx = x2 - x1
        dy = y2 - y1
        length = math.sqrt(dx**2 + dy**2)
        
        if length > 0:
            # Perpendicular vector
            perp_x = -dy / length
            perp_y = dx / length
            
            # Offset the label
            mid_x += perp_x * offset
            mid_y += perp_y * offset
    elif dist < 0.6:  # Somewhat close to center
        # Push radially outward
        if dist > 0:
            mid_x = mid_x / dist * (dist + offset)
            mid_y = mid_y / dist * (dist + offset)
    
    return mid_x, mid_y


def main():
    """Generate user overlap graph"""
    
    # Get app IDs
    try:
        with open('app_ids.txt', 'r') as f:
            app_ids = [int(line.strip()) for line in f if line.strip()]
    except FileNotFoundError:
        print("Error: app_ids.txt not found")
        sys.exit(1)
    
    if len(app_ids) < 2:
        print("Error: Need at least 2 games to calculate overlap")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"USER OVERLAP GRAPH GENERATOR")
    print(f"{'='*80}")
    print(f"Analyzing overlap for {len(app_ids)} games: {app_ids}")
    
    # Connect to database
    DB_URL = os.getenv('DB_URL')
    if not DB_URL:
        print("Error: DB_URL not found in .env file")
        sys.exit(1)
    
    db = DatabaseManager(DB_URL)
    db.connect()
    
    # Get game names and users
    print("\nFetching game data...")
    game_data = {}
    game_names = []
    
    for i, app_id in enumerate(app_ids):
        name = get_game_name(db, app_id)
        users = get_game_users(db, app_id)
        game_data[i] = {
            'app_id': app_id,
            'name': name,
            'users': users,
            'total_users': len(users)
        }
        game_names.append(name)
        print(f"  {name}: {len(users)} unique users")
    
    # Calculate overlaps
    print("\nCalculating overlaps...")
    overlaps = []
    overlap_matrix = {}
    
    for (idx1, idx2) in combinations(range(len(app_ids)), 2):
        game1 = game_data[idx1]
        game2 = game_data[idx2]
        
        overlap_count = calculate_overlap(game1['users'], game2['users'])
        
        if overlap_count > 0:
            # Calculate percentage relative to each game
            pct1 = (overlap_count / game1['total_users'] * 100) if game1['total_users'] > 0 else 0
            pct2 = (overlap_count / game2['total_users'] * 100) if game2['total_users'] > 0 else 0
            
            overlaps.append({
                'game1': game1['name'],
                'game2': game2['name'],
                'overlap': overlap_count,
                'pct1': pct1,
                'pct2': pct2
            })
            
            overlap_matrix[(idx1, idx2)] = overlap_count
            
            print(f"  {game1['name']} ↔ {game2['name']}: {overlap_count} users ({pct1:.1f}% / {pct2:.1f}%)")
    
    # Create visualization
    print("\nGenerating graph...")
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Get circular layout
    positions = create_circular_layout(len(app_ids))
    
    # Find max overlap for scaling
    max_overlap = max([o['overlap'] for o in overlaps]) if overlaps else 1
    max_users = max([g['total_users'] for g in game_data.values()])
    
    # Draw edges (connections between games)
    for (idx1, idx2), overlap_count in overlap_matrix.items():
        x1, y1 = positions[idx1]
        x2, y2 = positions[idx2]
        
        # Line width based on overlap
        width = 1 + (overlap_count / max_overlap) * 5
        
        # Draw line
        ax.plot([x1, x2], [y1, y2], 'gray', linewidth=width, alpha=0.5, zorder=1)
        
        # Calculate offset label position (SMART LABELING)
        label_x, label_y = get_label_offset(x1, y1, x2, y2, offset=0.15)
        
        ax.text(label_x, label_y, str(overlap_count), 
                fontsize=8, ha='center', va='center',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8, edgecolor='none'),
                zorder=3)
    
    # Draw nodes (games)
    for idx, game in game_data.items():
        x, y = positions[idx]
        
        # Node size based on user count
        size = 200 + (game['total_users'] / max_users) * 1000
        
        # Draw circle
        circle = plt.Circle((x, y), radius=0.15, color='lightblue', 
                           edgecolor='black', linewidth=2, alpha=0.7, zorder=2)
        ax.add_patch(circle)
        
        # Add game name and user count
        ax.text(x, y, f"{game['name']}\n({game['total_users']} users)", 
                fontsize=9, ha='center', va='center', fontweight='bold', zorder=4)
    
    # Set axis properties
    ax.set_xlim(-1.8, 1.8)
    ax.set_ylim(-1.8, 1.8)
    ax.set_aspect('equal')
    ax.axis('off')
    
    plt.title("User Overlap Between Games\n(Circle size = total users, Line width = overlap count)", 
              fontsize=14, fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    # Save the graph
    output_file = 'user_overlap_graph.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"\n💾 Graph saved to: {output_file}")
    plt.close()
    
    # Create a detailed overlap table
    print("\n" + "="*80)
    print("OVERLAP SUMMARY")
    print("="*80)
    print(f"{'Game 1':<30} {'Game 2':<30} {'Overlap':<10} {'% of G1':<10} {'% of G2':<10}")
    print("-"*80)
    
    # Sort by overlap count
    overlaps_sorted = sorted(overlaps, key=lambda x: x['overlap'], reverse=True)
    for overlap in overlaps_sorted:
        print(f"{overlap['game1']:<30} {overlap['game2']:<30} "
              f"{overlap['overlap']:<10} {overlap['pct1']:<10.1f} {overlap['pct2']:<10.1f}")
    
    # Export to CSV
    csv_file = 'user_overlap_data.csv'
    with open(csv_file, 'w') as f:
        f.write("game_1,game_2,users_with_both,game_1_pct_overlap,game_2_pct_overlap\n")
        for overlap in overlaps_sorted:
            f.write(f'"{overlap["game1"]}","{overlap["game2"]}",{overlap["overlap"]},'
                   f'{overlap["pct1"]:.2f},{overlap["pct2"]:.2f}\n')
    
    print(f"\n💾 Overlap data saved to: {csv_file}")
    
    db.disconnect()
    
    print(f"\n{'='*80}")
    print(f"Analysis complete!")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()