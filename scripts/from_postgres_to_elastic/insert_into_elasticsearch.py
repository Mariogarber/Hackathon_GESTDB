import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging
import datetime
import math
import time as time_module
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'), 
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT')
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ES_URL = "http://elasticsearch:9200"

ES_INDEX_VIDEOS = 'videos'
ES_INDEX_COMMENTS = 'comments'
ES_INDEX_CHANNELS = 'channels'

def _safe_int(value, default=0):
    """Safely convert value to integer"""
    if value is None:
        return default
    try:
        if isinstance(value, float) and math.isnan(value):
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default

def _safe_str(value, default=''):
    """Safely convert value to string"""
    if value is None:
        return default
    return str(value)

def _safe_iso_date(value):
    """Safely convert value to ISO date format"""
    if value is None:
        return None
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    try:
        return str(value)
    except Exception:
        return None

def convert_time_to_seconds(value):
    """Convert timedelta/time/string/num to seconds"""
    if value is None:
        return 0
    if isinstance(value, datetime.timedelta):
        return int(value.total_seconds())
    if isinstance(value, datetime.time):
        return value.hour * 3600 + value.minute * 60 + value.second
    if isinstance(value, (int, float)) and not (isinstance(value, float) and math.isnan(value)):
        return int(value)
    if isinstance(value, str):
        try:
            parts = value.split(':')
            if len(parts) == 3:
                h, m, s = [int(float(p)) for p in parts]
                return h*3600 + m*60 + s
        except Exception:
            return 0
    return 0

MAPPING_VIDEOS = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "title_raw": {"type": "text", "analyzer": "standard", "fields": {"keyword": {"type": "keyword"}}},
            "duration_seconds": {"type": "integer"},
            "topic": {"type": "text", "analyzer": "standard"},
            "published_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "view_count": {"type": "integer"},
            "like_count": {"type": "integer"},
            "language": {"type": "keyword"},
            "description": {"type": "text", "analyzer": "standard"},
            "id_channel": {"type": "keyword"}, 
            "title_embedding": {"type": "dense_vector","dims": 384, "index": True,"similarity": "cosine"}, 
            "description_embedding": {"type": "dense_vector","dims": 384, "index": True,"similarity": "cosine"}, 
            "topic_embedding":{"type": "dense_vector","dims": 384, "index": True,"similarity": "cosine"}
        }
    },
    "settings": {"number_of_shards": 1, "number_of_replicas": 0}
}

MAPPING_COMMENTS = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "id_video": {"type": "keyword"},
            "text": {"type": "text", "analyzer": "standard"},
            "published_at": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
            "like_count": {"type": "integer"},
            "sentiment_score": {"type": "integer"}, 
            "comment_embedding": {"type": "dense_vector","dims": 384, "index": True,"similarity": "cosine"}, 
        }
    },
    "settings": {"number_of_shards": 1, "number_of_replicas": 0}
}

MAPPING_CHANNELS = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "name": {"type": "text", "analyzer": "standard", "fields": {"keyword": {"type": "keyword"}}},
            "language": {"type": "keyword"},
            "description": {"type": "text", "analyzer": "standard"},
            "suscriber_count": {"type": "integer"},
            "banner": {"type": "keyword"},
            "category_link": {"type": "keyword"}
        }
    },
    "settings": {"number_of_shards": 1, "number_of_replicas": 0}
}

def connect_es_with_retries(url, retries=6, wait_seconds=5):
    """Connect to Elasticsearch with retry logic"""
    for attempt in range(1, retries+1):
        try:
            es_client = Elasticsearch(url)
            if es_client.ping():
                logger.info("Connected to Elasticsearch (attempt %d)", attempt)
                return es_client
            else:
                logger.warning("Elasticsearch ping failed (attempt %d)", attempt)
        except Exception as error:
            logger.warning("Error connecting to ES (attempt %d): %r", attempt, error)
        time_module.sleep(wait_seconds)
    raise ConnectionError("Could not connect to Elasticsearch after multiple attempts")

def create_index_if_not_exists(es_client, index_name, mapping):
    """Create Elasticsearch index if it doesn't exist"""
    try:
        if not es_client.indices.exists(index=index_name):
            es_client.indices.create(index=index_name, body=mapping)
            logger.info("Index '%s' created successfully", index_name)
        else:
            logger.info("Index '%s' already exists", index_name)
    except Exception as error:
        logger.error("Error creating index '%s': %r", index_name, error)
        raise

def create_indices(es_client):
    """Create all required Elasticsearch indices"""
    create_index_if_not_exists(es_client, ES_INDEX_VIDEOS, MAPPING_VIDEOS)
    create_index_if_not_exists(es_client, ES_INDEX_COMMENTS, MAPPING_COMMENTS)
    create_index_if_not_exists(es_client, ES_INDEX_CHANNELS, MAPPING_CHANNELS)

def fetch_videos_from_postgres():
    """Fetch videos data from PostgreSQL"""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        query = """
        SELECT 
            id,
            title_raw,
            duration,
            topic,
            published_at,
            view_count,
            like_count,
            language,
            description,
            id_channel
        FROM public.video
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        connection.close()

        embeddings1 = pd.read_csv('/app/data/embeddings_data/videos_embeddings.csv')
        embeddings1 = embeddings1[["id", "title_embedding", "description_embedding", "topic_embedding"]]

        df_sql["id"] = df_sql["id"].astype(str)
        emb_df["id"] = emb_df["id"].astype(str)
        merged = df_sql.merge(
            emb_df[["id"] + expected_emb_cols].drop_duplicates(subset=["id"]),
            on="id",
            how="left"
        )


        columns.extend(["title_embedding", "description_embedding", "topic_embedding"])
        rows = [row + tuple(embeddings1.iloc[i]) for i, row in enumerate(rows)]
        videos = [dict(zip(columns, row)) for row in rows]
        logger.info("Retrieved %d videos from PostgreSQL", len(videos))
        return videos
    except Exception as error:
        logger.error("Error fetching videos from PostgreSQL: %r", error)
        raise
def fetch_videos_from_postgres2():
    """Fetch videos data from PostgreSQL"""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        
        query = """
        SELECT 
            id,
            title_raw,
            duration,
            topic,
            published_at,
            view_count,
            like_count,
            language,
            description,
            id_channel
        FROM public.video
        """
        
        # 1) Leer tabla SQL a DataFrame
        df_sql = pd.read_sql(query, connection)
        connection.close()

        # 2) Leer CSV tal cual (sin parseos)
        emb_df = pd.read_csv('/app/data/embeddings_data/videos_embeddings.csv')

        # Asegurar que las columnas esperadas existen en el CSV (si no, las creamos vacías)
        expected_emb_cols = ["title_embedding", "description_embedding", "topic_embedding"]

        # 3) Merge por id si existe en el CSV (coerción a str para evitar mismatches simples)
        df_sql["id"] = df_sql["id"].astype(str)
        emb_df["id"] = emb_df["id"].astype(str)
        merged = df_sql.merge(
            emb_df[["id"] + expected_emb_cols].drop_duplicates(subset=["id"]),
            on="id",
            how="left"
        )

        # 5) Devolver lista de dicts (sin modificar embeddings)
        return merged.to_dict(orient="records")
    except Exception as error:
        logger.error("Error fetching videos from PostgreSQL: %r", error)
        raise

def fetch_comments_from_postgres():
    """Fetch comments data from PostgreSQL"""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        
        query = """
        SELECT 
            id,
            id_video,
            published_at,
            text,
            like_count,
            sentiment_score
        FROM public.comment
        """
        
        df_sql = pd.read_sql(query, connection)
        connection.close()

        embeddings1 = pd.read_csv('/app/data/embeddings_data/comments_embeddings_part1.csv')
        embeddings1 = embeddings1[["id","comment_embedding"]]
        embeddings2 =pd.read_csv('/app/data/embeddings_data/comments_embeddings_part2.csv')
        embeddings2 = embeddings2[["id","comment_embedding"]]
        embeddings3 = pd.read_csv('/app/data/embeddings_data/comments_embeddings_part3.csv')
        embeddings3 = embeddings3[["id","comment_embedding"]]
        embeddings = pd.concat([embeddings1, embeddings2, embeddings3], ignore_index=True)

        expected_emb_cols = ["comment_embedding"]
        df_sql["id"] = df_sql["id"].astype(str)
        embeddings["id"] = embeddings["id"].astype(str)
        merged = df_sql.merge(
            embeddings[["id"] + expected_emb_cols].drop_duplicates(subset=["id"]),
            on="id",
            how="left"
        )
        
        return merged.to_dict(orient="records")
    except Exception as error:
        logger.error("Error fetching comments from PostgreSQL: %r", error)
        raise

def fetch_channels_from_postgres():
    """Fetch channels data from PostgreSQL"""
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        query = """
        SELECT 
            id,
            name,
            language,
            description,
            suscriber_count,
            banner,
            category_link
        FROM public.channel
        """
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        
        channels = [dict(zip(columns, row)) for row in rows]
        logger.info("Retrieved %d channels from PostgreSQL", len(channels))
        return channels
    except Exception as error:
        logger.error("Error fetching channels from PostgreSQL: %r", error)
        raise

def parse_embedding_string(embedding_str):
    """Parse embedding string to list of floats"""
    if embedding_str is None:
        return []
    try:
        embedding_str = embedding_str.strip('[]')  # elimina corchetes al inicio/final
        parts = embedding_str.split(',')
        return [float(x) for x in parts]
    except Exception:
        return []

def index_videos_bulk(es_client, videos, batch_size=500):
    """Index videos data to Elasticsearch in bulk"""
    actions = []
    total_indexed = 0
    error_count = 0
    
    EXPECTED_DIMS = 384 # Define la dimensión esperada

    for video in videos:
        try:
            document_id = _safe_str(video.get('id')) # Obtener ID primero para logs
            
            title_embedding = parse_embedding_string(video.get('title_embedding'))
            description_embedding = parse_embedding_string(video.get('description_embedding'))
            topic_embedding = parse_embedding_string(video.get('topic_embedding'))

            
            # Comprobar si la dimensión es incorrecta
            if len(title_embedding) != EXPECTED_DIMS:
                title_embedding = None
            # Comprobar si es un vector de magnitud cero
            elif all(v == 0.0 for v in title_embedding):
                logger.warning("Video %s: 'title_embedding' es un vector cero. Se anulará.", document_id)
                title_embedding = None

            if len(description_embedding) != EXPECTED_DIMS:
                description_embedding = None
            elif all(v == 0.0 for v in description_embedding):
                logger.warning("Video %s: 'description_embedding' es un vector cero. Se anulará.", document_id)
                description_embedding = None

            if len(topic_embedding) != EXPECTED_DIMS:
                topic_embedding = None
            elif all(v == 0.0 for v in topic_embedding):
                logger.warning("Video %s: 'topic_embedding' es un vector cero. Se anulará.", document_id)
                topic_embedding = None
            
            document = {
                'id': document_id,
                'title_raw': _safe_str(video.get('title_raw')),
                'duration_seconds': convert_time_to_seconds(video.get('duration')),
                'topic': _safe_str(video.get('topic')),
                'published_at': _safe_iso_date(video.get('published_at')),
                'view_count': _safe_int(video.get('view_count')),
                'like_count': _safe_int(video.get('like_count')),
                'language': _safe_str(video.get('language')),
                'id_channel': _safe_str(video.get('id_channel')),
                'description': _safe_str(video.get('description')),
                'title_embedding': title_embedding,
                'description_embedding': description_embedding,
                'topic_embedding': topic_embedding
            }
            actions.append({"_index": ES_INDEX_VIDEOS, "_id": document_id, "_source": document})

            if len(actions) >= batch_size:
                try:
                    success_count, failed_docs = bulk(es_client, actions, request_timeout=60, raise_on_error=False)
                    total_indexed += success_count
                    if failed_docs:
                         logger.warning("%d document(s) failed to index. Sample error: %s", len(failed_docs), failed_docs[0])
                         error_count += len(failed_docs)

                except Exception as bulk_error:
                    logger.error("Error during bulk operation: %r", bulk_error)
                    error_count += len(actions)
                
                actions = [] # Limpiar lote
                logger.info("Indexed %d videos so far", total_indexed)

        except Exception as error:
            error_count += 1
            logger.error("Error preparing video %s: %r", video.get('id', 'unknown'), error)

    if actions:
        try:
            success_count, failed_docs = bulk(es_client, actions, request_timeout=60, raise_on_error=False)
            total_indexed += success_count
            if failed_docs:
                logger.warning("%d final document(s) failed to index. Sample error: %s", len(failed_docs), failed_docs[0])
                error_count += len(failed_docs)
        except Exception as error:
            error_count += len(actions)
            logger.error("Error in final videos bulk operation: %r", error)

    return total_indexed, error_count

def index_comments_bulk(es_client, comments, batch_size=500):
    """Index comments data to Elasticsearch in bulk"""
    actions = []
    total_indexed = 0
    error_count = 0
    
    EXPECTED_DIMS = 384 # Define la dimensión esperada

    for comment in comments:
        try:
            document_id = _safe_str(comment.get('id'))
            comment_embedding = parse_embedding_string(comment.get('comment_embedding'))
            
            # Comprobar si la dimensión es incorrecta (ej. [])
            if len(comment_embedding) != EXPECTED_DIMS:
                comment_embedding = None
            # Comprobar si es un vector de magnitud cero
            elif all(v == 0.0 for v in comment_embedding):
                logger.warning("Comment %s: 'comment_embedding' es un vector cero. Se anulará.", document_id)
                comment_embedding = None
                
            document = {
                'id': document_id,
                'id_video': _safe_str(comment.get('id_video')),
                'text': _safe_str(comment.get('text')),
                'published_at': _safe_iso_date(comment.get('published_at')),
                'like_count': _safe_int(comment.get('like_count')),
                'sentiment_score': _safe_int(comment.get('sentiment_score')),
                'comment_embedding': comment_embedding
            }
            actions.append({"_index": ES_INDEX_COMMENTS, "_id": document_id, "_source": document})

            if len(actions) >= batch_size:
                try:
                    success_count, failed_docs = bulk(es_client, actions, request_timeout=60, raise_on_error=False)
                    total_indexed += success_count
                    if failed_docs:
                         logger.warning("%d comment(s) failed to index. Sample error: %s", len(failed_docs), failed_docs[0])
                         error_count += len(failed_docs)
                except Exception as bulk_error:
                    logger.error("Error during comments bulk operation: %r", bulk_error)
                    error_count += len(actions)
                
                actions = []
                logger.info("Indexed %d comments so far", total_indexed)

        except Exception as error:
            error_count += 1
            logger.error("Error preparing comment %s: %r", comment.get('id', 'unknown'), error)

    if actions:
        try:
            success_count, failed_docs = bulk(es_client, actions, request_timeout=60, raise_on_error=False)
            total_indexed += success_count
            if failed_docs:
                logger.warning("%d final comment(s) failed to index. Sample error: %s", len(failed_docs), failed_docs[0])
                error_count += len(failed_docs)
        except Exception as error:
            error_count += len(actions)
            logger.error("Error in final comments bulk operation: %r", error)

    return total_indexed, error_count

def index_channels_bulk(es_client, channels, batch_size=500):
    """Index channels data to Elasticsearch in bulk"""
    actions = []
    total_indexed = 0
    error_count = 0
    
    for channel in channels:
        try:
            document_id = _safe_str(channel.get('id'))
            document = {
                'id': document_id,
                'name': _safe_str(channel.get('name')),
                'language': _safe_str(channel.get('language')),
                'description': _safe_str(channel.get('description')),
                'suscriber_count': _safe_int(channel.get('suscriber_count')),
                'banner': _safe_str(channel.get('banner')),
                'category_link': _safe_str(channel.get('category_link')),
            }
            actions.append({"_index": ES_INDEX_CHANNELS, "_id": document_id, "_source": document})

            if len(actions) >= batch_size:
                success_count, _ = bulk(es_client, actions, request_timeout=60)
                total_indexed += success_count
                actions = []
                logger.info("Indexed %d channels so far", total_indexed)

        except Exception as error:
            error_count += 1
            logger.error("Error preparing channel %s: %r", channel.get('id', 'unknown'), error)

    if actions:
        try:
            success_count, _ = bulk(es_client, actions, request_timeout=60)
            total_indexed += success_count
        except Exception as error:
            error_count += len(actions)
            logger.error("Error in final channels bulk operation: %r", error)

    return total_indexed, error_count

def sync_postgres_to_elasticsearch():
    """Main synchronization function from PostgreSQL to Elasticsearch"""
    logger.info("Starting PostgreSQL to Elasticsearch synchronization")

    es_client = connect_es_with_retries(ES_URL, retries=6, wait_seconds=5)
    create_indices(es_client)

    videos = fetch_videos_from_postgres2()
    comments = fetch_comments_from_postgres()
    channels = fetch_channels_from_postgres()

    videos_indexed, videos_errors = index_videos_bulk(es_client, videos)
    comments_indexed, comments_errors = index_comments_bulk(es_client, comments)
    channels_indexed, channels_errors = index_channels_bulk(es_client, channels)

    try:
        es_client.indices.refresh(index=ES_INDEX_VIDEOS)
        es_client.indices.refresh(index=ES_INDEX_COMMENTS)
        es_client.indices.refresh(index=ES_INDEX_CHANNELS)
    except Exception as error:
        logger.warning("Could not force refresh: %r", error)

    try:
        videos_count = es_client.count(index=ES_INDEX_VIDEOS)['count']
    except Exception:
        videos_count = None
    try:
        comments_count = es_client.count(index=ES_INDEX_COMMENTS)['count']
    except Exception:
        comments_count = None
    try:
        channels_count = es_client.count(index=ES_INDEX_CHANNELS)['count']
    except Exception:
        channels_count = None

    logger.info("Synchronization completed:")
    logger.info("Videos indexed: %d (errors: %d) - Total in ES: %s", videos_indexed, videos_errors, str(videos_count))
    logger.info("Comments indexed: %d (errors: %d) - Total in ES: %s", comments_indexed, comments_errors, str(comments_count))
    logger.info("Channels indexed: %d (errors: %d) - Total in ES: %s", channels_indexed, channels_errors, str(channels_count))

if __name__ == "__main__":
    sync_postgres_to_elasticsearch()