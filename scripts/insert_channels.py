import json
import psycopg2
import time
import os
import sys
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'), 
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT')
}

def wait_for_postgres(max_retries=12, delay=5):
    """Wait for PostgreSQL to be available"""
    logging.info("Waiting for PostgreSQL to be available...")
    for retry_count in range(max_retries):
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            connection.close()
            logging.info("PostgreSQL is available")
            return True
        except psycopg2.OperationalError as error:
            if "connection" in str(error).lower():
                logging.info(f"Attempt {retry_count + 1}/{max_retries}: Waiting for PostgreSQL...")
                time.sleep(delay)
            else:
                logging.error(f"Connection error: {error}")
                return False
    logging.error("Timeout: PostgreSQL is not available")
    return False

def check_table_exists(cursor):
    """Check if the channel table exists in the public schema"""
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'channel'
            );
        """)
        table_exists = cursor.fetchone()[0]
        if table_exists:
            logging.info("Table 'public.channel' found")
        else:
            logging.error("Table 'public.channel' does NOT exist")
        return table_exists
    except Exception as error:
        logging.error(f"Error checking table: {error}")
        return False

def insert_channels_from_json(file_path):
    try:
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return False
            
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        logging.info(f"Reading {len(data)} channels from {file_path}")
        
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        if not check_table_exists(cursor):
            logging.error("Cannot continue without table 'public.channel'")
            return False
        
        inserted_count = 0
        updated_count = 0
        error_count = 0
        
        for channel_name, channel_info in data.items():
            try:
                channel_id = channel_info['id']
                name = channel_info['name']
                language = channel_info.get('language', '') or '' 
                description = channel_info.get('description', '')
                subscriber_count = channel_info['subscriber_count']
                banner = channel_info['banner']
                category_link = channel_info.get('custom_url', '') or channel_info.get('handle', '')
                
                if not channel_id or not name or subscriber_count is None or not banner:
                    logging.warning(f"Missing required fields for {name}, skipping...")
                    error_count += 1
                    continue
                
                insert_query = """
                INSERT INTO public.channel (id, name, language, description, suscriber_count, banner, category_link)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    language = EXCLUDED.language,
                    description = EXCLUDED.description,
                    suscriber_count = EXCLUDED.suscriber_count,
                    banner = EXCLUDED.banner,
                    category_link = EXCLUDED.category_link
                """
                
                cursor.execute(insert_query, (
                    channel_id, name, language, description, 
                    subscriber_count, banner, category_link
                ))
                
                if cursor.statusmessage.startswith('INSERT'):
                    inserted_count += 1
                    logging.info(f"INSERTED: {name}")
                else:
                    updated_count += 1
                    logging.info(f"UPDATED: {name}")
                    
            except Exception as error:
                error_count += 1
                logging.error(f"ERROR processing {channel_info.get('name', 'Unknown')}: {error}")
                connection.rollback()
                continue
        
        connection.commit()
        
        logging.info(f" STATISTICS:")
        logging.info(f"  Inserted: {inserted_count}")
        logging.info(f"  Updated: {updated_count}") 
        logging.info(f"  Errors: {error_count}")
        logging.info(f"  Total processed: {inserted_count + updated_count + error_count}")
        
        return error_count == 0
        
    except json.JSONDecodeError as error:
        logging.error(f"JSON error: {error}")
        return False
    except psycopg2.Error as error:
        logging.error(f"Database error: {error}")
        if 'connection' in locals():
            connection.rollback()
        return False
    except Exception as error:
        logging.error(f"Unexpected error: {error}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    logging.info("Starting YouTube channels importer")
    logging.info("=" * 50)
    
    json_file = "/app/data/api_data/channels_data.json"
    
    if not os.path.exists(json_file):
        logging.error(f"File not found: {json_file}")
        logging.info("Contents of /app/data directory:")
        try:
            logging.info(str(os.listdir('/app/data')))
        except Exception as error:
            logging.error(f"Could not list directory: {error}")
        sys.exit(1)
    
    if wait_for_postgres():
        success = insert_channels_from_json(json_file)
        if success:
            logging.info("\nImport completed successfully!")
            sys.exit(0)
        else:
            logging.warning("\nImport completed with errors!")
            sys.exit(1)
    else:
        logging.error("\nCould not connect to PostgreSQL")
        sys.exit(1)