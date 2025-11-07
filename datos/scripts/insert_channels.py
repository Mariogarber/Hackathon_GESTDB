import json
import psycopg2
import time
import os
import sys

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

def check_table_exists(cursor):
    """Verificar si la tabla channel existe en el esquema public"""
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'channel'
            );
        """)
        exists = cursor.fetchone()[0]
        if exists:
            print("✅ Tabla 'public.channel' encontrada")
        else:
            print("❌ Tabla 'public.channel' NO existe")
        return exists
    except Exception as e:
        print(f"❌ Error verificando tabla: {e}")
        return False

def insert_channels_from_json(file_path):
    try:
        # Verificar que el archivo existe
        if not os.path.exists(file_path):
            print(f"❌ Archivo no encontrado: {file_path}")
            return False
            
        # Leer el archivo JSON
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        print(f"📖 Leyendo {len(data)} canales desde {file_path}")
        
        # Conectar a la base de datos
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Verificar que la tabla existe
        if not check_table_exists(cursor):
            print("❌ No se puede continuar sin la tabla 'public.channel'")
            return False
        
        # Contadores para estadísticas
        inserted = 0
        updated = 0
        errors = 0
        
        # Insertar/actualizar cada canal
        for channel_name, channel_info in data.items():
            try:
                # Preparar datos según la estructura EXACTA de tu tabla
                channel_id = channel_info['id']
                name = channel_info['name']
                language = channel_info.get('language', '') or ''  # Default a 'es' si está vacío
                description = channel_info.get('description', '')
                suscriber_count = channel_info['subscriber_count']
                banner = channel_info['banner']
                category_link = channel_info.get('custom_url', '') or channel_info.get('handle', '')
                
                # Validar campos NOT NULL
                if not channel_id or not name or suscriber_count is None or not banner:
                    print(f"⚠️  Campos requeridos faltantes en {name}, saltando...")
                    errors += 1
                    continue
                
                # Query de inserción - CON ESQUEMA PUBLIC
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
                
                # Ejecutar inserción
                cursor.execute(insert_query, (
                    channel_id,
                    name,
                    language,
                    description,
                    suscriber_count,
                    banner,
                    category_link
                ))
                
                # Verificar si fue INSERT o UPDATE
                if cursor.statusmessage.startswith('INSERT'):
                    inserted += 1
                    print(f"✅ INSERT: {name}")
                else:
                    updated += 1
                    print(f"🔄 UPDATE: {name}")
                    
            except Exception as e:
                errors += 1
                print(f"❌ ERROR en {channel_info.get('name', 'Unknown')}: {e}")
                # En caso de error, hacer rollback de la transacción actual
                conn.rollback()
                continue
        
        # Confirmar cambios
        conn.commit()
        
        # Mostrar estadísticas
        print(f"\n📊 ESTADÍSTICAS:")
        print(f"   ✅ Insertados: {inserted}")
        print(f"   🔄 Actualizados: {updated}") 
        print(f"   ❌ Errores: {errors}")
        print(f"   📈 Total procesados: {inserted + updated + errors}")
        
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
    print("🚀 Iniciando importador de canales de YouTube")
    print("=" * 50)
    
    json_file = "/app/data/canales_youtube_info.json"
    
    # Verificar archivo
    if not os.path.exists(json_file):
        print(f"❌ Archivo no encontrado: {json_file}")
        print("📁 Contenido del directorio /app/data:")
        try:
            print(os.listdir('/app/data'))
        except:
            print("No se pudo listar el directorio")
        sys.exit(1)
    
    # Esperar a PostgreSQL e importar
    if wait_for_postgres():
        success = insert_channels_from_json(json_file)
        if success:
            print("\n🎉 ¡Importación completada exitosamente!")
            sys.exit(0)
        else:
            print("\n💥 ¡Importación completada con errores!")
            sys.exit(1)
    else:
        print("\n💥 No se pudo conectar a PostgreSQL")
        sys.exit(1)