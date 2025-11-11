import json
import psycopg2
import os
import logging
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'), 
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT')
}

def load_categories_from_json(file_path):
    """Load categories from JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            categories = json.load(file)
        logging.info(f"Loaded {len(categories)} categories from {file_path}")
        return categories
    except Exception as e:
        logging.error(f"Error loading JSON file: {e}")
        return []

def connect_to_database():
    """Connect to PostgreSQL database"""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        logging.info("Successfully connected to the database")
        return connection
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None

def insert_categories(connection, categories):
    """Insert categories into the category table"""
    if not categories:
        logging.warning("No categories to insert")
        return
    
    category_data = []
    for category in categories:
        category_data.append((category.get('id'), category.get('title', '')))
    
    # Insert query
    insert_query = """
    INSERT INTO public.category (id, name) VALUES %s ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
    """
    
    try:
        cursor = connection.cursor()
        execute_values(cursor, insert_query, category_data)
        connection.commit()
        logging.info(f"Successfully inserted/updated {len(category_data)} categories in the database")
        cursor.close()
    except Exception as e:
        connection.rollback()
        logging.error(f"Error inserting categories: {e}")

def main():
    """Main function"""
    json_file_path = "/app/data/categories_data.json"
    
    if not os.path.exists(json_file_path):
        logging.error(f"File not found: {json_file_path}")
        return
    
    categories = load_categories_from_json(json_file_path)
    
    if not categories:
        return
    
    connection = connect_to_database()
    if not connection:
        return
    
    insert_categories(connection, categories)
    
    connection.close()
    logging.info("Category import process completed successfully")

if __name__ == "__main__":
    main()