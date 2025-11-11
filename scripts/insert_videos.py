import json
import psycopg2
from datetime import datetime
import logging
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'), 
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT')
}

def safe_int_convert(value, default=0):
    """Safely convert a value to integer"""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def convert_published_at(published_at):
    """Convert published_at to datetime format for PostgreSQL"""
    if published_at is None:
        return datetime.now()
    
    try:
        if isinstance(published_at, str):
            published_at_str = published_at.replace('Z', '+00:00')
            return datetime.fromisoformat(published_at_str)
        
        elif isinstance(published_at, (int, float)):
            return datetime.fromtimestamp(published_at / 1000)
        
        elif isinstance(published_at, datetime):
            return published_at
        
        else:
            logger.warning(f"Unrecognized date format: {published_at}, using current date")
            return datetime.now()
            
    except Exception as error:
        logger.warning(f"Error converting date {published_at}: {error}, using current date")
        return datetime.now()

def process_video_data(video_data):
    """Process video data for database insertion"""
    try:
        summary = video_data.get('summary', '')
        if not summary:
            summary = video_data['title']
        
        thumbnails = video_data.get('thumbnails', {})
        thumbnail_url = "No thumbnail available"
        
        if (isinstance(thumbnails, dict) and 
            'default' in thumbnails and 
            isinstance(thumbnails['default'], dict) and
            'url' in thumbnails['default']):
            thumbnail_url = thumbnails['default']['url']
        
        view_count = safe_int_convert(video_data.get('view_count'))
        like_count = safe_int_convert(video_data.get('like_count'))
        comment_count = safe_int_convert(video_data.get('comment_count'))
        
        duration = video_data.get('duration', 0)
        if isinstance(duration, str):
            try:
                duration = int(duration)
            except (ValueError, TypeError):
                duration = 0
        
        published_at = convert_published_at(video_data.get('published_at'))
        
        return {
            'id': video_data['video_id'],
            'title_raw': video_data['title'], 
            'title_processed': video_data['title'],
            'description': video_data.get('description', ''),  
            'published_at': published_at,
            'language': video_data.get('language', 'en'),
            'duration': duration,
            'view_count': view_count,
            'like_count': like_count,
            'thumbnails': thumbnail_url,
            'comment_count': comment_count,
            'topic': summary[:500],
            'id_channel': video_data['id_channel'], 
            'id_category': video_data['video_category_id']
        }
    except Exception as error:
        logger.error(f"Error processing video {video_data.get('video_id', 'unknown')}: {error}")
        raise

def insert_videos_from_json_file(json_file_path):
    """Insert videos from JSON file with robust error handling"""
    
    try:
        with open(json_file_path, 'r', encoding='utf-8') as file:
            videos_data = json.load(file)
        
        if isinstance(videos_data, list):
            videos_list = videos_data
        else:
            videos_list = [videos_data]
        
        logger.info(f"Processing file: {json_file_path}")
        logger.info(f"Found {len(videos_list)} videos")
        
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        for index, video_data in enumerate(videos_list, 1):
            connection = None
            cursor = None
            try:
                connection = psycopg2.connect(**DB_CONFIG)
                cursor = connection.cursor()
                
                processed_data = process_video_data(video_data)
                
                insert_query = """
                INSERT INTO public.video (
                    id, title_raw, title_processed, description, published_at, 
                    language, duration, view_count, like_count, thumbnails, 
                    comment_count, topic, id_channel, id_category
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (id) DO NOTHING
                """
                
                cursor.execute(insert_query, (
                    processed_data['id'],
                    processed_data['title_raw'],
                    processed_data['title_processed'],
                    processed_data['description'],
                    processed_data['published_at'],
                    processed_data['language'],
                    processed_data['duration'],
                    processed_data['view_count'],
                    processed_data['like_count'],
                    processed_data['thumbnails'],
                    processed_data['comment_count'],
                    processed_data['topic'],
                    processed_data['id_channel'], 
                    processed_data['id_category']
                ))
                
                connection.commit()
                
                if cursor.rowcount > 0:
                    inserted_count += 1
                    logger.debug(f"Inserted video: {processed_data['id']}")
                else:
                    duplicate_count += 1
                    logger.debug(f"Duplicate video: {processed_data['id']}")
                
                if index % 100 == 0:
                    logger.info(f"Processed {index}/{len(videos_list)} videos...")
                
            except psycopg2.IntegrityError as error:
                if connection:
                    connection.rollback()
                logger.warning(f"Duplicate video: {video_data.get('video_id', 'unknown')}")
                duplicate_count += 1
            except Exception as error:
                if connection:
                    connection.rollback()
                logger.error(f"Error processing video {video_data.get('video_id', 'unknown')}: {error}")
                error_count += 1
            finally:
                if cursor:
                    cursor.close()
                if connection:
                    connection.close()
        
        logger.info(f"File {json_file_path} processing completed:")
        logger.info(f"- Inserted: {inserted_count}")
        logger.info(f"- Duplicates: {duplicate_count}")
        logger.info(f"- Errors: {error_count}")
        
        return inserted_count
        
    except FileNotFoundError:
        logger.error(f"JSON file not found: {json_file_path}")
        return 0
    except json.JSONDecodeError as error:
        logger.error(f"Invalid JSON format in file {json_file_path}: {error}")
        return 0
    except Exception as error:
        logger.error(f"Error processing file {json_file_path}: {error}")
        return 0

if __name__ == "__main__":
    json_file_path = "/app/data/videos_data.json"
    logger.info("Starting video data import process")
    total_inserted = insert_videos_from_json_file(json_file_path)
    logger.info(f"TOTAL: {total_inserted} videos inserted successfully")