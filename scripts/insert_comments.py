import pandas as pd
import psycopg2
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
    if pd.isna(value) or value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default

def safe_bool_convert(value, default=False):
    """Safely convert a value to boolean"""
    if pd.isna(value) or value is None:
        return default
    try:
        return bool(int(float(value)))
    except (ValueError, TypeError):
        return default

def process_csv_and_insert():
    """Process CSV file and insert data into database"""
    try:
        df = pd.read_csv('/app/data/comments_data.csv', encoding='utf-8')
        logging.info(f"Successfully loaded CSV file with {len(df)} records")
    except Exception as error:
        logging.error(f"Error loading CSV file: {error}")
        return
    
    connection = None
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        logging.info("Connected to database successfully")
        
        total_records = len(df)
        inserted_count = 0
        error_count = 0
        
        for index, row in df.iterrows():
            try:
                comment_id = row['id']
                text = row['text'] if pd.notna(row['text']) else ""
                
                published_at_str = str(row['published_at']) if pd.notna(row['published_at']) else ""
                published_at = datetime.now().date()  # Default value
                
                try:
                    if 'T' in published_at_str:
                        published_at = datetime.strptime(published_at_str.split('T')[0], '%Y-%m-%d').date()
                    else:
                        published_at = datetime.strptime(published_at_str, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    published_at = datetime.now().date()
                
                like_count = safe_int_convert(row['like_count'])
                is_positive = safe_bool_convert(row['is_possitive'])
                video_id = row['id_video'] if pd.notna(row['id_video']) else ""
                
                if not comment_id or not video_id:
                    logging.warning(f"Row {index + 1}: Missing required fields, skipping...")
                    error_count += 1
                    continue
                
                insert_query = """
                INSERT INTO public.comment 
                (id, text, published_at, like_count, is_possitive, id_video) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """
                
                cursor.execute(insert_query, (
                    comment_id, text, published_at, like_count, 
                    is_positive, video_id
                ))
                
                inserted_count += 1
                
                if inserted_count % 1000 == 0:
                    connection.commit()
                    logging.info(f"Processed {inserted_count} records...")
                
            except Exception as error:
                if connection:
                    connection.rollback()
                error_count += 1
                logging.error(f"Error processing row {index + 1}: {error}")
                continue
        
        if connection:
            connection.commit()
        
        logging.info(f"\nProcessing completed:")
        logging.info(f"Total records: {total_records}")
        logging.info(f"Records inserted: {inserted_count}")
        logging.info(f"Errors: {error_count}")
        
        success_rate = (inserted_count / total_records) * 100 if total_records > 0 else 0
        logging.info(f"Success rate: {success_rate:.2f}%")
        
    except psycopg2.Error as error:
        logging.error(f"Database connection error: {error}")
        if connection:
            connection.rollback()
    except Exception as error:
        logging.error(f"Unexpected error: {error}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            cursor.close()
            connection.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    logging.info("Starting CSV processing and database insertion")
    process_csv_and_insert()
    logging.info("CSV processing completed")