import json
import psycopg2
import os
from psycopg2.extras import execute_values

def load_categories_from_json(file_path):
    """Carga las categorías desde el archivo JSON"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            categories = json.load(file)
        print(f"✅ Cargadas {len(categories)} categorías desde {file_path}")
        return categories
    except Exception as e:
        print(f"❌ Error cargando el archivo JSON: {e}")
        return []

def connect_to_db():
    """Conecta a la base de datos PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="bbdd_api_youtube",
            user="postgres",
            password="postgres",
            port="5432"
        )
        print("✅ Conectado a la base de datos")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a la base de datos: {e}")
        return None

def insert_categories(conn, categories):
    """Inserta las categorías en la tabla category"""
    if not categories:
        print("⚠️ No hay categorías para insertar")
        return
    
    # Preparar los datos para inserción
    category_data = []
    for category in categories:
        category_data.append((
            category.get('id'),
            category.get('title', '')
        ))
    
    # Query de inserción
    insert_query = """
    INSERT INTO public.category (id, name)
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name
    """
    
    try:
        cursor = conn.cursor()
        execute_values(cursor, insert_query, category_data)
        conn.commit()
        print(f"✅ Insertadas/actualizadas {len(category_data)} categorías en la base de datos")
        cursor.close()
    except Exception as e:
        conn.rollback()
        print(f"❌ Error insertando categorías: {e}")

def main():
    """Función principal"""
    # Ruta al archivo JSON
    json_file_path = "/app/data/categorias_video_youtube.json"
    
    # Verificar si el archivo existe
    if not os.path.exists(json_file_path):
        print(f"❌ Archivo no encontrado: {json_file_path}")
        return
    
    # Cargar categorías desde JSON
    categories = load_categories_from_json(json_file_path)
    
    if not categories:
        return
    
    # Conectar a la base de datos
    conn = connect_to_db()
    if not conn:
        return
    
    # Insertar categorías
    insert_categories(conn, categories)
    
    # Cerrar conexión
    conn.close()
    print("✅ Proceso de importación de categorías completado")

if __name__ == "__main__":
    main()