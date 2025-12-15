#!/usr/bin/env python3
"""
Sentiment Analysis for Steam Reviews
Analyzes reviews from database using local Ollama instance
"""

import json
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
import os
import sys
from dotenv import load_dotenv

from openai import OpenAI

# Load environment variables
load_dotenv()

# Import database manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database_manager import DatabaseManager


class SentimentType(Enum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"


@dataclass
class AnalysisResult:
    """Data class to hold analysis results for each chunk"""
    chunk: str
    sentiment: SentimentType
    confidence: float
    category: str
    is_out_of_box: bool
    suggested_category: Optional[str] = None
    tags: List[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk": self.chunk,
            "sentiment": self.sentiment.value,
            "confidence": self.confidence,
            "category": self.category,
            "is_out_of_box": self.is_out_of_box,
            "suggested_category": self.suggested_category,
            "tags": self.tags or []
        }


class TextAnalyzer:
    """Main analyzer class for managing API client and predefined categories"""

    def __init__(self, client: OpenAI, predefined_categories: List[str]):
        self.client = client
        self.predefined_categories = predefined_categories

    def analyze_text(self, text: str) -> Dict[str, Any]:
        """Main pipeline function"""
        chunks = chunk_text(text)
        results = [self._analyze_chunk(chunk) for chunk in chunks]

        return {
            "original_text": text,
            "total_chunks": len(chunks),
            "analysis_results": [result.to_dict() for result in results],
            "summary": self._generate_summary(results)
        }

    def _analyze_chunk(self, chunk: str) -> AnalysisResult:
        """Analyze individual chunk using OpenAI API"""
        try:
            response = self.client.chat.completions.create(
                model="gemma3:4b",
                messages=[
                    {
                        "role": "system",
                        "content": f"""You are a sentiment analysis and categorization expert. 
                        Analyze the given text chunk and respond with ONLY a valid JSON object.
                        
                        Predefined categories: {', '.join(self.predefined_categories)}
                        
                        Response format:
                        {{
                            "sentiment": "positive|negative|neutral",
                            "confidence": 0.0-1.0,
                            "category": "category_name or 'out-of-box'",
                            "suggested_category": "suggestion if out-of-box, null otherwise",
                            "tags": ["tag1", "tag2", "tag3"]
                        }}"""
                    },
                    {
                        "role": "user",
                        "content": f"Analyze this text chunk: '{chunk}'"
                    }
                ],
                response_format={"type": "json_object"},
                temperature=0.3
            )

            result_json = json.loads(response.choices[0].message.content)

            return AnalysisResult(
                chunk=chunk,
                sentiment=SentimentType(result_json["sentiment"]),
                confidence=result_json["confidence"],
                category=result_json["category"],
                is_out_of_box=result_json["category"] == "out-of-box",
                suggested_category=result_json.get("suggested_category"),
                tags=result_json.get("tags", [])
            )

        except Exception as e:
            raise e

    def _generate_summary(self, results: List[AnalysisResult]) -> Dict[str, Any]:
        """Generate summary statistics from analysis results"""
        total = len(results)
        sentiment_counts = {sentiment.value: 0 for sentiment in SentimentType}
        out_of_box_count = 0
        categories = {}

        for result in results:
            sentiment_counts[result.sentiment.value] += 1
            if result.is_out_of_box:
                out_of_box_count += 1

            category = result.category
            categories[category] = categories.get(category, 0) + 1

        return {
            "total_chunks": total,
            "sentiment_distribution": sentiment_counts,
            "out_of_box_count": out_of_box_count,
            "category_distribution": categories,
            "average_confidence": sum(r.confidence for r in results) / total if total > 0 else 0.0
        }


def chunk_text(text: str, chunk_size: int = 100, overlap: int = 20) -> List[str]:
    """Split text into relevant chunks for analysis using sliding window approach"""
    text = normalize_text(text)
    sentences = split_into_sentences(text)

    if all(len(sentence.split()) <= chunk_size for sentence in sentences):
        return [s.strip() for s in sentences if s.strip()]

    words = text.split()
    chunks = []

    for i in range(0, len(words), chunk_size - overlap):
        chunk_words = words[i:i + chunk_size]
        chunk = ' '.join(chunk_words)
        chunk = align_to_sentence_boundary(chunk)

        if chunk.strip():
            chunks.append(chunk.strip())

    return chunks


def normalize_text(text: str) -> str:
    """Clean and normalize input text"""
    text = re.sub(r'\s+', ' ', text.strip())
    text = re.sub(r'\s+([.!?])', r'\1', text)
    return text


def split_into_sentences(text: str) -> List[str]:
    """Split text into sentences using regex patterns"""
    sentence_pattern = r'(?<=[.!?])\s+(?=[A-Z])'
    sentences = re.split(sentence_pattern, text)
    return [s.strip() for s in sentences if s.strip()]


def align_to_sentence_boundary(chunk: str) -> str:
    """Try to align chunk end to sentence boundary"""
    sentence_endings = ['.', '!', '?']

    for i in range(len(chunk) - 1, -1, -1):
        if chunk[i] in sentence_endings:
            if i == len(chunk) - 1 or chunk[i + 1].isspace():
                return chunk[:i + 1]

    return chunk


def insert_sentiment_data(db, analysis_data, review_id, game_name, user_id):
    """Insert sentiment analysis data into database"""
    try:
        print(f"    📝 Saving to database...")
        print(f"       Review ID: {review_id}")
        print(f"       Game: {game_name}")
        print(f"       User: {user_id}")
        print(f"       Chunks: {analysis_data['total_chunks']}")
        
        # Insert main sentiment analysis record
        query = """
        INSERT INTO sentiment_analysis (
            original_text, total_chunks, created_at, from_game, user_id, source_platform
        ) VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        
        db.cursor.execute(query, (
            analysis_data['original_text'],
            analysis_data['total_chunks'],
            datetime.now().isoformat(),
            game_name,
            user_id,
            'steam'
        ))
        
        sentiment_id = db.cursor.fetchone()[0]
        print(f"       ✅ Created sentiment_analysis record (ID: {sentiment_id})")
        
        # Insert analysis chunks
        chunk_count = 0
        for idx, chunk_data in enumerate(analysis_data['analysis_results']):
            chunk_query = """
            INSERT INTO analysis_chunks (
                sentiment_analysis_id, chunk, sentiment, confidence, category,
                is_out_of_box, suggested_category, tags, chunk_order
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            db.cursor.execute(chunk_query, (
                sentiment_id,
                chunk_data['chunk'],
                chunk_data['sentiment'],
                chunk_data['confidence'],
                chunk_data['category'],
                chunk_data['is_out_of_box'],
                chunk_data['suggested_category'],
                json.dumps(chunk_data['tags']),
                idx
            ))
            chunk_count += 1
        
        print(f"       ✅ Inserted {chunk_count} analysis_chunks records")
        
        # Insert summary
        summary = analysis_data['summary']
        summary_query = """
        INSERT INTO analysis_summaries (
            sentiment_analysis_id, total_chunks, positive_count, negative_count,
            neutral_count, out_of_box_count, average_confidence, category_distribution
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        db.cursor.execute(summary_query, (
            sentiment_id,
            summary['total_chunks'],
            summary['sentiment_distribution']['positive'],
            summary['sentiment_distribution']['negative'],
            summary['sentiment_distribution']['neutral'],
            summary['out_of_box_count'],
            summary['average_confidence'],
            json.dumps(summary['category_distribution'])
        ))
        
        print(f"       ✅ Created analysis_summaries record")
        print(f"       Summary: {summary['sentiment_distribution']['positive']} positive, "
              f"{summary['sentiment_distribution']['negative']} negative, "
              f"{summary['sentiment_distribution']['neutral']} neutral")
        
        db.conn.commit()
        print(f"       ✅ Transaction committed successfully")
        return True
        
    except Exception as e:
        print(f"       ❌ Database error: {e}")
        db.conn.rollback()
        print(f"       ⚠️  Transaction rolled back")
        return False


def main():
    """Main function to run sentiment analysis"""
    
    # Predefined categories
    PREDEFINED_CATEGORIES = [
        "Graphics fidelity", "Art style", "Runtime smoothness", "Specs requirement",
        "Developer reputation", "Uniqueness", "UI/UX", "Difficulty", "Balance",
        "Game Mechanics", "Narrative quality", "Modding support", "Pricing",
        "Replayability", "General experience", "Memes", "Sound design",
        "Music quality", "Voice acting", "Community support", "Bug fixes",
        "Content updates", "Multiplayer features", "Tutorial quality",
        "Accessibility", "Controller support", "Platform optimization",
        "Loading times", "Save system", "Customization options",
        "Achievement system", "Progression system", "Monetization model",
        "DLC value", "Early access state"
    ]
    
    # Get app IDs to process
    try:
        with open('app_ids.txt', 'r') as f:
            app_ids = [int(line.strip()) for line in f if line.strip()]
    except FileNotFoundError:
        print("Error: app_ids.txt not found")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"SENTIMENT ANALYSIS - Steam Reviews")
    print(f"{'='*80}")
    print(f"Processing sentiment analysis for {len(app_ids)} apps: {app_ids}")
    
    # Connect to database
    DB_URL = os.getenv('DB_URL')
    if not DB_URL:
        print("Error: DB_URL not found in .env file")
        sys.exit(1)
    
    db = DatabaseManager(DB_URL)
    db.connect()
    
    # Create analyzer (localhost only)
    API_KEY = os.getenv('OPENAI_API_KEY', 'dummy-key')
    OLLAMA_URL = os.getenv('OLLAMA_URL', 'http://localhost:11434/v1')
    
    print(f"Ollama URL: {OLLAMA_URL}")
    print(f"Model: gemma3:4b")
    print(f"{'='*80}\n")
    
    analyzer = TextAnalyzer(
        OpenAI(base_url=OLLAMA_URL, api_key=API_KEY),
        PREDEFINED_CATEGORIES
    )
    
    # Fetch reviews to analyze
    placeholders = ','.join(['%s'] * len(app_ids))
    query = f"""
    SELECT 
        sr.id,
        sr.review_id,
        sr.review,
        sr.author_steamid,
        sr.steam_product_id,
        sp.name as game_name
    FROM steam_reviews sr
    LEFT JOIN steam_products sp ON sr.steam_product_id = sp.steam_app_id
    WHERE sr.steam_product_id IN ({placeholders})
    AND sr.review IS NOT NULL
    """
    
    db.cursor.execute(query, tuple(app_ids))
    reviews = db.cursor.fetchall()
    
    print(f"📊 Found {len(reviews)} reviews to analyze\n")
    
    # Process reviews sequentially
    results = []
    success_count = 0
    failed_count = 0
    
    for idx, row in enumerate(reviews, 1):
        review_data = {
            'id': row[0],
            'review_id': row[1],
            'review_text': row[2],
            'author_steamid': row[3],
            'steam_product_id': row[4],
            'game_name': row[5] if row[5] else str(row[4])
        }
        
        print(f"\n{'='*80}")
        print(f"🔍 Processing review {idx}/{len(reviews)}")
        print(f"{'='*80}")
        print(f"  Review ID: {review_data['review_id']}")
        print(f"  Game: {review_data['game_name']}")
        print(f"  Author: {review_data['author_steamid']}")
        print(f"  Text length: {len(review_data['review_text'])} characters")
        
        try:
            # Analyze the review
            print(f"  🤖 Analyzing with Ollama...")
            result = analyzer.analyze_text(review_data['review_text'])
            print(f"  ✅ Analysis complete")
            print(f"     Chunks analyzed: {result['total_chunks']}")
            print(f"     Sentiment distribution: {result['summary']['sentiment_distribution']}")
            
            # Save to database
            if insert_sentiment_data(
                db,
                result,
                review_data['review_id'],
                review_data['game_name'],
                review_data['author_steamid']
            ):
                results.append(result)
                success_count += 1
                print(f"  ✅ SAVED SUCCESSFULLY")
            else:
                failed_count += 1
                print(f"  ❌ SAVE FAILED")
            
            # Progress summary every 10 reviews
            if idx % 10 == 0:
                print(f"\n{'─'*80}")
                print(f"📈 Progress Update: {idx}/{len(reviews)} reviews processed")
                print(f"   ✅ Successful: {success_count}")
                print(f"   ❌ Failed: {failed_count}")
                print(f"{'─'*80}")
                
        except Exception as e:
            failed_count += 1
            print(f"  ❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
    
    # Save results to JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"sentiment_analysis_{timestamp}.json"
    
    try:
        with open(output_filename, 'w', encoding='utf-8') as file:
            json.dump(results, file, indent=2, ensure_ascii=False)
        print(f"\n💾 Results saved to {output_filename}")
    except Exception as e:
        print(f"\n❌ Failed to save results: {e}")
    
    db.disconnect()
    
    print(f"\n{'='*80}")
    print(f"SENTIMENT ANALYSIS COMPLETE")
    print(f"{'='*80}")
    print(f"  ✅ Successful: {success_count}")
    print(f"  ❌ Failed: {failed_count}")
    print(f"  📊 Total: {len(reviews)}")
    print(f"  📁 Output: {output_filename}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()