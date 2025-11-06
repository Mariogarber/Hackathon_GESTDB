import json
import psycopg2
import time
import os
import sys
from datetime import datetime, timedelta

# Configuración para Docker
DB_CONFIG = {
    'host': 'postgres',
    'database': 'bbdd_api_youtube', 
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'
}

def wait_for_postgres(max_retries=12, delay=5):
    """Esperar a que PostgreSQL esté disponible"""
    print("⏳ Esperando a que PostgreSQL esté disponible...")
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("✅ PostgreSQL está disponible")
            return True
        except psycopg2.OperationalError as e:
            if "connection" in str(e).lower():
                print(f"🔄 Intento {i+1}/{max_retries}: Esperando PostgreSQL...")
                time.sleep(delay)
            else:
                print(f"❌ Error de conexión: {e}")
                return False
    print("❌ Timeout: PostgreSQL no está disponible")
    return False

def convert_duration(seconds):
    """Convertir segundos a formato time HH:MM:SS"""
    return str(timedelta(seconds=seconds))

def convert_published_at(published_at_str):
    """Convertir fecha ISO a formato date"""
    try:
        # Convertir "2025-11-06T16:00:39Z" a "2025-11-06"
        return datetime.fromisoformat(published_at_str.replace('Z', '+00:00')).date()
    except Exception as e:
        print(f"❌ Error convirtiendo fecha {published_at_str}: {e}")
        return None

def load_summary_data(summary_file_path):
    """Cargar los datos de summary desde el archivo JSON"""
    try:
        if not os.path.exists(summary_file_path):
            print(f"⚠️  Archivo de summary no encontrado: {summary_file_path}")
            return {}
            
        with open(summary_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Crear diccionario con video_id como clave y summary como valor
        summary_dict = {}
        for item in data:
            video_id = item.get('video_id')
            summary = item.get('summary', '')
            if video_id and summary:
                summary_dict[video_id] = summary
        
        print(f"📖 Cargados {len(summary_dict)} summaries desde {summary_file_path}")
        return summary_dict
        
    except Exception as e:
        print(f"❌ Error cargando summary data: {e}")
        return {}

def safe_insert_category(cursor, category_id, category_name):
    """Insertar categoría de forma segura"""
    try:
        # Primero verificar si la categoría ya existe
        cursor.execute("SELECT COUNT(*) FROM public.category WHERE id = %s", (category_id,))
        category_exists = cursor.fetchone()[0] > 0
        
        if category_exists:
            # Actualizar categoría existente
            update_query = "UPDATE public.category SET name = %s WHERE id = %s"
            cursor.execute(update_query, (category_name, category_id))
            return "updated"
        else:
            # Insertar nueva categoría
            insert_query = "INSERT INTO public.category (id, name) VALUES (%s, %s)"
            cursor.execute(insert_query, (category_id, category_name))
            return "inserted"
    except Exception as e:
        print(f"❌ Error manejando categoría {category_id}: {e}")
        return "error"

def safe_insert_video_category(cursor, video_id, category_id):
    """Insertar relación video-categoría de forma segura"""
    try:
        # Verificar si la relación ya existe
        cursor.execute(
            "SELECT COUNT(*) FROM public.video_category WHERE id_video = %s AND id_category = %s", 
            (video_id, category_id)
        )
        relation_exists = cursor.fetchone()[0] > 0
        
        if not relation_exists:
            # Insertar nueva relación (sin ON CONFLICT porque no hay constraint única en estas columnas)
            insert_query = "INSERT INTO public.video_category (id_video, id_category) VALUES (%s, %s)"
            cursor.execute(insert_query, (video_id, category_id))
            return "inserted"
        else:
            return "exists"
    except Exception as e:
        print(f"❌ Error insertando relación video-categoría {video_id}-{category_id}: {e}")
        return "error"

def check_video_exists_before_insert(cursor, video_id):
    """Verificar si un video existe antes de insertarlo"""
    try:
        cursor.execute("SELECT COUNT(*) FROM public.video WHERE id = %s", (video_id,))
        return cursor.fetchone()[0] > 0
    except Exception as e:
        print(f"❌ Error verificando existencia del video {video_id}: {e}")
        return False

def insert_video_data(video_file_path, summary_file_path):
    try:
        # Verificar que el archivo de videos existe
        if not os.path.exists(video_file_path):
            print(f"❌ Archivo no encontrado: {video_file_path}")
            return False
            
        # Cargar datos de summary
        summary_data = load_summary_data(summary_file_path)
            
        # Leer el archivo JSON de videos
        with open(video_file_path, 'r', encoding='utf-8') as file:
            video_data = json.load(file)
        
        print(f"📖 Procesando {len(video_data)} videos desde {video_file_path}")
        
        # Conectar a la base de datos
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Contadores para estadísticas
        videos_inserted = 0
        videos_updated = 0
        categories_inserted = 0
        categories_updated = 0
        video_categories_inserted = 0
        errors = 0
        
        # Procesar cada video
        for video_item in video_data:
            try:
                video_id = video_item['video_id']
                
                # Obtener el topic/summary para este video
                topic = summary_data.get(video_id, '')[:500]  # Limitar longitud si es necesario
                
                # 1. Manejar categoría si existe
                category_id = video_item.get('video_category_id')
                category_name = video_item.get('video_category')
                
                category_result = None
                if category_id and category_name:
                    category_result = safe_insert_category(cursor, category_id, category_name)
                    if category_result == "inserted":
                        categories_inserted += 1
                        print(f"📂 INSERT categoría: {category_name}")
                    elif category_result == "updated":
                        categories_updated += 1
                
                # 2. Preparar datos para la tabla video
                title_raw = video_item['title']
                title_processed = video_item['title']  # Mismo que title_raw si no hay procesamiento
                description = video_item['description']
                published_at = convert_published_at(video_item['published_at'])
                language = video_item['language']
                duration = convert_duration(video_item['duration'])
                view_count = video_item['view_count']
                like_count = video_item['like_count']
                comment_count = video_item['comment_count']
                thumbnails = json.dumps(video_item['thumbnails'])  # Convertir a JSON string
                id_channel = video_item['id_channel']
                
                # Validar campos requeridos
                if not published_at:
                    print(f"⚠️  Fecha inválida en video {video_id}, saltando...")
                    errors += 1
                    continue
                
                # 3. Verificar si el video ya existe
                video_exists = check_video_exists_before_insert(cursor, video_id)
                
                if video_exists:
                    # UPDATE del video existente
                    video_query = """
                    UPDATE public.video SET
                        title_raw = %s,
                        title_processed = %s,
                        description = %s,
                        published_at = %s,
                        language = %s,
                        duration = %s,
                        view_count = %s,
                        like_count = %s,
                        thumbnails = %s,
                        comment_count = %s,
                        id_channel = %s,
                        topic = %s
                    WHERE id = %s
                    """
                    cursor.execute(video_query, (
                        title_raw, title_processed, description, published_at,
                        language, duration, view_count, like_count, thumbnails,
                        comment_count, id_channel, topic, video_id
                    ))
                    videos_updated += 1
                    print(f"🔄 UPDATE video: {title_raw[:50]}...")
                else:
                    # INSERT del nuevo video
                    video_query = """
                    INSERT INTO public.video (
                        id, title_raw, title_processed, description, published_at, 
                        language, duration, view_count, like_count, thumbnails, 
                        comment_count, id_channel, topic
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(video_query, (
                        video_id, title_raw, title_processed, description, published_at,
                        language, duration, view_count, like_count, thumbnails,
                        comment_count, id_channel, topic
                    ))
                    videos_inserted += 1
                    print(f"✅ INSERT video: {title_raw[:50]}... (Topic: {topic[:50] if topic else 'Sin topic'}...)")
                
                # 4. Insertar relación en video_category si existe categoría
                if category_id:
                    relation_result = safe_insert_video_category(cursor, video_id, category_id)
                    if relation_result == "inserted":
                        video_categories_inserted += 1
                        print(f"🔗 INSERT relación: {video_id} -> {category_id}")
                
            except Exception as e:
                errors += 1
                print(f"❌ ERROR procesando video {video_item.get('video_id', 'Unknown')}: {e}")
                # Hacer rollback solo de esta transacción
                conn.rollback()
                continue
        
        # Confirmar todos los cambios
        conn.commit()
        
        # Mostrar estadísticas
        print(f"\n📊 ESTADÍSTICAS FINALES:")
        print(f"   ✅ Videos insertados: {videos_inserted}")
        print(f"   🔄 Videos actualizados: {videos_updated}")
        print(f"   📂 Categorías insertadas: {categories_inserted}")
        print(f"   🔄 Categorías actualizadas: {categories_updated}")
        print(f"   🔗 Relaciones video-categoría insertadas: {video_categories_inserted}")
        print(f"   📝 Videos con topic: {sum(1 for vid in video_data if summary_data.get(vid['video_id']))}")
        print(f"   ❌ Errores: {errors}")
        print(f"   📈 Total procesado: {videos_inserted + videos_updated + errors}")
        
        return errors == 0
        
    except json.JSONDecodeError as e:
        print(f"❌ Error en JSON: {e}")
        return False
    except psycopg2.Error as e:
        print(f"❌ Error de base de datos: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("🚀 Iniciando importador de videos de YouTube")
    print("=" * 50)
    
    video_json_file = "/app/data/videos_filtrados_por_categoria.json"
    summary_json_file = "/app/data/videos_filtrados_por_categoria_con_resumen.json"
    
    # Verificar archivos
    if not os.path.exists(video_json_file):
        print(f"❌ Archivo no encontrado: {video_json_file}")
        print("📁 Contenido del directorio /app/data:")
        try:
            print(os.listdir('/app/data'))
        except:
            print("No se pudo listar el directorio")
        sys.exit(1)
    
    # Esperar a PostgreSQL e importar
    if wait_for_postgres():
        success = insert_video_data(video_json_file, summary_json_file)
        if success:
            print("\n🎉 ¡Importación de videos completada exitosamente!")
            sys.exit(0)
        else:
            print("\n💥 ¡Importación completada con errores!")
            sys.exit(1)
    else:
        print("\n💥 No se pudo conectar a PostgreSQL")
        sys.exit(1)