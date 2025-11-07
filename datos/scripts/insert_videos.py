import json
import psycopg2
from datetime import datetime, timedelta

# Datos de conexión a PostgreSQL
DB_CONFIG = {
    'host': 'postgres',
    'database': 'bbdd_api_youtube', 
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'
}

def convert_timestamp_to_date(timestamp_ms):
    """Convierte timestamp en milisegundos a fecha"""
    return datetime.fromtimestamp(timestamp_ms / 1000).date()

def convert_seconds_to_time(seconds):
    """Convierte segundos a tiempo"""
    return (datetime.min + timedelta(seconds=seconds)).time()

def process_video_data(video_data):
    """Procesa los datos del video para insertar en la BD"""
    # Limitar longitudes para evitar problemas con constraints de la BD
    description = video_data.get('description', '')
    if len(description) > 5000:
        description = description[:5000]
    
    summary = video_data.get('summary', '')
    if len(summary) > 500:
        summary = summary[:500]
    
    return {
        'id': video_data['video_id'],
        'title_raw': video_data['title'][:500],  # Limitar título
        'title_processed': video_data['title'][:500],
        'description': description,
        'published_at': convert_timestamp_to_date(video_data['published_at']),
        'language': video_data['language'],
        'duration': convert_seconds_to_time(video_data['duration']),
        'view_count': video_data['view_count'],
        'like_count': video_data['like_count'],
        'thumbnails': json.dumps(video_data['thumbnails']),
        'comment_count': video_data['comment_count'],
        'topic': summary,
        'id_channel': video_data['id_channel']
    }

def insert_videos_from_json_file(json_file_path):
    """Lee el archivo JSON e inserta los videos en la base de datos"""
    
    try:
        # Leer el archivo JSON
        with open(json_file_path, 'r', encoding='utf-8') as file:
            videos_data = json.load(file)
        
        # Si el JSON es una lista de objetos
        if isinstance(videos_data, list):
            videos_list = videos_data
        else:
            # Si es un solo objeto, lo convertimos en lista
            videos_list = [videos_data]
        
        print(f"Procesando archivo: {json_file_path}")
        print(f"Encontrados {len(videos_list)} videos en el archivo")
        
        # Conectar a la base de datos
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Contador para estadísticas
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        # Procesar e insertar cada video con su propia transacción
        for i, video_data in enumerate(videos_list, 1):
            cursor = None
            try:
                cursor = conn.cursor()
                processed_data = process_video_data(video_data)
                
                # SQL para insertar con manejo de duplicados
                insert_sql = """
                INSERT INTO public.video (
                    id, title_raw, title_processed, description, published_at, 
                    language, duration, view_count, like_count, thumbnails, 
                    comment_count, topic, id_channel
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (id) DO NOTHING
                """
                
                cursor.execute(insert_sql, (
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
                    processed_data['id_channel']
                ))
                
                # Confirmar cada inserción individualmente
                conn.commit()
                
                # Verificar si se insertó o fue duplicado
                if cursor.rowcount > 0:
                    inserted_count += 1
                else:
                    duplicate_count += 1
                
                # Mostrar progreso cada 100 videos
                if i % 100 == 0:
                    print(f"Procesados {i}/{len(videos_list)} videos...")
                
            except psycopg2.IntegrityError as e:
                # Hacer rollback de la transacción fallida
                if conn:
                    conn.rollback()
                print(f"Error de integridad en video {video_data.get('video_id', 'unknown')}: {e}")
                duplicate_count += 1
                continue
            except psycopg2.Error as e:
                # Hacer rollback de la transacción fallida
                if conn:
                    conn.rollback()
                print(f"Error de base de datos en video {video_data.get('video_id', 'unknown')}: {e}")
                error_count += 1
                continue
            except Exception as e:
                # Hacer rollback de la transacción fallida
                if conn:
                    conn.rollback()
                print(f"Error procesando video {video_data.get('video_id', 'unknown')}: {e}")
                error_count += 1
                continue
            finally:
                # Cerrar cursor después de cada video
                if cursor:
                    cursor.close()
        
        print(f"Archivo {json_file_path} completado:")
        print(f"- Videos insertados: {inserted_count}")
        print(f"- Duplicados omitidos: {duplicate_count}")
        print(f"- Errores: {error_count}")
        print(f"- Total procesados: {len(videos_list)}")
        print("-" * 50)
        
        return inserted_count
        
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {json_file_path}")
        return 0
    except json.JSONDecodeError as e:
        print(f"Error: El archivo JSON no tiene un formato válido: {e}")
        return 0
    except Exception as e:
        print(f"Error inesperado: {e}")
        return 0
    finally:
        if 'conn' in locals() and conn:
            conn.close()

# Versión alternativa con manejo más robusto de errores
def insert_videos_from_json_file_alternative(json_file_path):
    """Versión alternativa que maneja cada video como conexión independiente"""
    
    try:
        # Leer el archivo JSON
        with open(json_file_path, 'r', encoding='utf-8') as file:
            videos_data = json.load(file)
        
        if isinstance(videos_data, list):
            videos_list = videos_data
        else:
            videos_list = [videos_data]
        
        print(f"Procesando archivo: {json_file_path}")
        print(f"Encontrados {len(videos_list)} videos en el archivo")
        
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        # Procesar cada video con conexión independiente
        for i, video_data in enumerate(videos_list, 1):
            conn = None
            cursor = None
            try:
                # Nueva conexión para cada video
                conn = psycopg2.connect(**DB_CONFIG)
                cursor = conn.cursor()
                
                processed_data = process_video_data(video_data)
                
                insert_sql = """
                INSERT INTO public.video (
                    id, title_raw, title_processed, description, published_at, 
                    language, duration, view_count, like_count, thumbnails, 
                    comment_count, topic, id_channel
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (id) DO NOTHING
                """
                
                cursor.execute(insert_sql, (
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
                    processed_data['id_channel']
                ))
                
                conn.commit()
                
                if cursor.rowcount > 0:
                    inserted_count += 1
                else:
                    duplicate_count += 1
                
                if i % 100 == 0:
                    print(f"Procesados {i}/{len(videos_list)} videos...")
                
            except Exception as e:
                if conn:
                    conn.rollback()
                print(f"Error en video {video_data.get('video_id', 'unknown')}: {e}")
                error_count += 1
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
        
        print(f"Archivo {json_file_path} completado:")
        print(f"- Videos insertados: {inserted_count}")
        print(f"- Duplicados omitidos: {duplicate_count}")
        print(f"- Errores: {error_count}")
        print(f"- Total procesados: {len(videos_list)}")
        print("-" * 50)
        
        return inserted_count
        
    except Exception as e:
        print(f"Error procesando archivo {json_file_path}: {e}")
        return 0

# Ejecutar el script
if __name__ == "__main__":
    total_inserted = 0
    
    for i in range(0, 5):
        json_file_path = f"/app/data/videos_filtrados_por_categoria_con_resumen{i}.json"
        # Usar la versión alternativa que es más robusta
        inserted = insert_videos_from_json_file_alternative(json_file_path)
        total_inserted += inserted
    
    print(f"TOTAL GENERAL: {total_inserted} videos insertados")