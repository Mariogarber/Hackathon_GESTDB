import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import logging
import datetime
import math
import time as time_module

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL config
DB_CONFIG = {
    'host': 'postgres',
    'database': 'bbdd_api_youtube',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'
}

# Elasticsearch (ajusta si necesitas auth / TLS)
ES_URL = "http://elasticsearch:9200"

# Índices
ES_INDEX_VIDEOS = 'videos'
ES_INDEX_COMMENTS = 'comments'
ES_INDEX_CHANNELS = 'channels'


# ---------------------------
# Helpers de saneamiento
# ---------------------------
def _safe_int(value, default=0):
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
    if value is None:
        return default
    return str(value)

def _safe_iso_date(value):
    if value is None:
        return None
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    try:
        # si viene como string, asumimos que ya es ISO o parseable
        return str(value)
    except Exception:
        return None

def convert_time_to_seconds(value):
    """Convierte timedelta/time/string/num a segundos."""
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


# ---------------------------
# Mappings de índices
# ---------------------------
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
            "id_channel": {"type": "keyword"}
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
            "like_count": {"type": "integer"}
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


# ---------------------------
# Conexión ES con reintentos
# ---------------------------
def connect_es_with_retries(url, retries=6, wait_seconds=5):
    for attempt in range(1, retries+1):
        try:
            es = Elasticsearch(url)
            if es.ping():
                logger.info("Conectado a Elasticsearch (intento %d)", attempt)
                return es
            else:
                logger.warning("Ping a Elasticsearch falló (intento %d)", attempt)
        except Exception as e:
            logger.warning("Error conectando a ES (intento %d): %r", attempt, e)
        time_module.sleep(wait_seconds)
    raise ConnectionError("No se pudo conectar a Elasticsearch tras varios intentos")


# ---------------------------
# Crear índices (genéricos y específicos)
# ---------------------------
def create_index_if_not_exists(es_client, index_name, mapping):
    try:
        if not es_client.indices.exists(index=index_name):
            es_client.indices.create(index=index_name, body=mapping)
            logger.info("Índice '%s' creado", index_name)
        else:
            logger.info("Índice '%s' ya existe", index_name)
    except Exception as e:
        logger.error("Error creando índice '%s': %r", index_name, e)
        raise

def create_indices(es_client):
    create_index_if_not_exists(es_client, ES_INDEX_VIDEOS, MAPPING_VIDEOS)
    create_index_if_not_exists(es_client, ES_INDEX_COMMENTS, MAPPING_COMMENTS)
    create_index_if_not_exists(es_client, ES_INDEX_CHANNELS, MAPPING_CHANNELS)


# ---------------------------
# Fetch desde Postgres (corregidos)
# ---------------------------
def fetch_videos_from_postgres():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
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
            id_channel
        FROM public.video
        """
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()
        videos = [dict(zip(cols, r)) for r in rows]
        logger.info("Obtenidos %d videos", len(videos))
        return videos
    except Exception as e:
        logger.error("Error fetch_videos_from_postgres: %r", e)
        raise

def fetch_comments_from_postgres():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        # CORRECCIÓN: quitar la coma extra después de like_count
        query = """
        SELECT 
            id,
            id_video,
            published_at,
            text,
            like_count
        FROM public.comment
        """
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()
        comments = [dict(zip(cols, r)) for r in rows]
        logger.info("Obtenidos %d comentarios", len(comments))
        return comments
    except Exception as e:
        logger.error("Error fetch_comments_from_postgres: %r", e)
        raise

def fetch_channels_from_postgres():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
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
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
        conn.close()
        channels = [dict(zip(cols, r)) for r in rows]
        logger.info("Obtenidos %d canales", len(channels))
        return channels
    except Exception as e:
        logger.error("Error fetch_channels_from_postgres: %r", e)
        raise


# ---------------------------
# Bulk index functions
# ---------------------------
def index_videos_bulk(es_client, videos, batch_size=500):
    actions = []
    total = 0
    errors = 0
    for v in videos:
        try:
            doc_id = _safe_str(v.get('id'))
            doc = {
                'id': doc_id,
                'title_raw': _safe_str(v.get('title_raw')),
                'duration_seconds': convert_time_to_seconds(v.get('duration')),
                # si ya calculaste duration_seconds en BD, puedes usar v.get('duration_seconds')
                'topic': _safe_str(v.get('topic')),
                'published_at': _safe_iso_date(v.get('published_at')),
                'view_count': _safe_int(v.get('view_count')),
                'like_count': _safe_int(v.get('like_count')),
                'language': _safe_str(v.get('language')),
                'id_channel': _safe_str(v.get('id_channel'))
            }
            actions.append({"_index": ES_INDEX_VIDEOS, "_id": doc_id, "_source": doc})

            if len(actions) >= batch_size:
                ok, _ = bulk(es_client, actions, request_timeout=60)
                total += ok
                actions = []
                logger.info("Indexed %d videos so far", total)

        except Exception as e:
            errors += 1
            logger.error("Error preparando video %s: %r", v.get('id', 'unknown'), e)

    if actions:
        try:
            ok, _ = bulk(es_client, actions, request_timeout=60)
            total += ok
        except Exception as e:
            errors += len(actions)
            logger.error("Error en bulk final videos: %r", e)

    return total, errors


def index_comments_bulk(es_client, comments, batch_size=500):
    actions = []
    total = 0
    errors = 0
    for c in comments:
        try:
            doc_id = _safe_str(c.get('id'))
            doc = {
                'id': doc_id,
                'id_video': _safe_str(c.get('id_video')),
                'text': _safe_str(c.get('text')),
                'published_at': _safe_iso_date(c.get('published_at')),
                'like_count': _safe_int(c.get('like_count')),
            }
            actions.append({"_index": ES_INDEX_COMMENTS, "_id": doc_id, "_source": doc})

            if len(actions) >= batch_size:
                ok, _ = bulk(es_client, actions, request_timeout=60)
                total += ok
                actions = []
                logger.info("Indexed %d comments so far", total)

        except Exception as e:
            errors += 1
            logger.error("Error preparando comment %s: %r", c.get('id', 'unknown'), e)

    if actions:
        try:
            ok, _ = bulk(es_client, actions, request_timeout=60)
            total += ok
        except Exception as e:
            errors += len(actions)
            logger.error("Error en bulk final comments: %r", e)

    return total, errors


def index_channels_bulk(es_client, channels, batch_size=500):
    actions = []
    total = 0
    errors = 0
    for ch in channels:
        try:
            doc_id = _safe_str(ch.get('id'))
            doc = {
                'id': doc_id,
                'name': _safe_str(ch.get('name')),
                'language': _safe_str(ch.get('language')),
                'description': _safe_str(ch.get('description')),
                'suscriber_count': _safe_int(ch.get('suscriber_count')),
                'banner': _safe_str(ch.get('banner')),
                'category_link': _safe_str(ch.get('category_link')),
            }
            actions.append({"_index": ES_INDEX_CHANNELS, "_id": doc_id, "_source": doc})

            if len(actions) >= batch_size:
                ok, _ = bulk(es_client, actions, request_timeout=60)
                total += ok
                actions = []
                logger.info("Indexed %d channels so far", total)

        except Exception as e:
            errors += 1
            logger.error("Error preparando channel %s: %r", ch.get('id', 'unknown'), e)

    if actions:
        try:
            ok, _ = bulk(es_client, actions, request_timeout=60)
            total += ok
        except Exception as e:
            errors += len(actions)
            logger.error("Error en bulk final channels: %r", e)

    return total, errors


# ---------------------------
# Función principal
# ---------------------------
def sync_postgres_to_elasticsearch():
    logger.info("Iniciando sincronización Postgres -> Elasticsearch")

    es = connect_es_with_retries(ES_URL, retries=6, wait_seconds=5)

    # crear índices si no existen
    create_indices(es)

    # obtener datos
    videos = fetch_videos_from_postgres()
    comments = fetch_comments_from_postgres()
    channels = fetch_channels_from_postgres()

    # indexar
    v_ok, v_err = index_videos_bulk(es, videos)
    c_ok, c_err = index_comments_bulk(es, comments)
    ch_ok, ch_err = index_channels_bulk(es, channels)

    # refresh
    try:
        es.indices.refresh(index=ES_INDEX_VIDEOS)
        es.indices.refresh(index=ES_INDEX_COMMENTS)
        es.indices.refresh(index=ES_INDEX_CHANNELS)
    except Exception as e:
        logger.warning("No se pudo forzar refresh: %r", e)

    # conteos finales
    try:
        cnt_v = es.count(index=ES_INDEX_VIDEOS)['count']
    except Exception:
        cnt_v = None
    try:
        cnt_c = es.count(index=ES_INDEX_COMMENTS)['count']
    except Exception:
        cnt_c = None
    try:
        cnt_ch = es.count(index=ES_INDEX_CHANNELS)['count']
    except Exception:
        cnt_ch = None

    logger.info("Sincronización finalizada:")
    logger.info("Videos indexed: %d (errors: %d)  - Total in ES: %s", v_ok, v_err, str(cnt_v))
    logger.info("Comments indexed: %d (errors: %d) - Total in ES: %s", c_ok, c_err, str(cnt_c))
    logger.info("Channels indexed: %d (errors: %d) - Total in ES: %s", ch_ok, ch_err, str(cnt_ch))


if __name__ == "__main__":
    sync_postgres_to_elasticsearch()