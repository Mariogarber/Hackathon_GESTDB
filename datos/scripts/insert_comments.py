import pandas as pd
import psycopg2
from datetime import datetime
import csv

# Configuración de la base de datos
DB_CONFIG = {
    'host': 'postgres',
    'database': 'bbdd_api_youtube', 
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'
}

def procesar_csv_y_insertar():
    # Leer el CSV
    df = pd.read_csv('/app/data/comments_data_final.csv', encoding='utf-8')
    
    # Conectar a la base de datos
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Contadores para estadísticas
        total_registros = len(df)
        insertados = 0
        errores = 0
        
        for index, row in df.iterrows():
            try:
                # Procesar cada campo según la estructura de tu tabla
                id_comment = row['id']
                text = row['text']
                
                # Convertir published_at a date
                published_at_str = row['published_at']
                if 'T' in published_at_str:
                    published_at = datetime.strptime(published_at_str.split('T')[0], '%Y-%m-%d').date()
                else:
                    published_at = datetime.strptime(published_at_str, '%Y-%m-%d').date()
                
                # is_response - asumimos False por defecto (puedes ajustar según tu lógica)
                is_response = False
                
                # like_count - convertir a entero, manejar valores nulos o vacíos
                like_count_str = str(row['like_count']).strip()
                like_count = int(float(like_count_str)) if like_count_str and like_count_str != 'nan' else 0
                
                # is_possitive - análisis básico de sentimiento
                text_lower = str(text).lower() if pd.notna(text) else ""
                #positive_words = ['gracias', 'buen', 'bueno', 'excelente', 'interesante', 'genial', 'perfecto', 'awesome', 'great', 'good']
                #is_possitive = any(word in text_lower for word in positive_words)
                is_possitive = row['is_positive']
                
                # is_formal - análisis básico de formalidad
                formal_indicators = ['saludos', 'estimado', 'cordiales', 'agradecido', 'respecto a', 'dear', 'regards']
                informal_indicators = ['xd', 'lol', 'jaja', 'jajaja', 'bro', 'amigo', 'haha', 'lmao']
                
                is_formal = (any(indicator in text_lower for indicator in formal_indicators) and 
                           not any(indicator in text_lower for indicator in informal_indicators))
                
                # id_channel - extraer del id_video o usar un valor por defecto
                id_channel = 'default_channel'  # Ajusta según tu lógica
                
                # id_video
                id_video = row['id_video']
                
                # Insertar en la base de datos
                insert_query = """
                INSERT INTO public.comment 
                (id, text, published_at, is_response, like_count, is_possitive, is_formal, id_channel, id_video)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_query, (
                    id_comment, text, published_at, is_response, like_count, 
                    is_possitive, is_formal, id_channel, id_video
                ))
                
                insertados += 1
                
                # Hacer commit cada 1000 registros para evitar transacciones muy largas
                if insertados % 1000 == 0:
                    conn.commit()
                    print(f"Procesados {insertados} registros...")
                
            except Exception as e:
                # Si hay error, hacer rollback de la transacción actual
                conn.rollback()
                errores += 1
                print(f"Error procesando fila {index + 1}: {e}")
                # Continuar con la siguiente fila
                continue
        
        # Confirmar los cambios finales
        conn.commit()
        
        print(f"\nProcesamiento completado:")
        print(f"Total de registros: {total_registros}")
        print(f"Registros insertados: {insertados}")
        print(f"Errores: {errores}")
        
    except Exception as e:
        print(f"Error de conexión a la base de datos: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

# Versión alternativa con manejo de transacciones por lote
def procesar_csv_por_lotes():
    """Alternativa procesando por lotes más pequeños"""
    df = pd.read_csv('/app/data/comments_data_final.csv', encoding='utf-8')
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        total_registros = len(df)
        insertados = 0
        errores = 0
        lote_size = 500  # Procesar en lotes de 500
        
        for i in range(0, total_registros, lote_size):
            lote = df.iloc[i:i + lote_size]
            registros_lote = []
            
            for index, row in lote.iterrows():
                try:
                    # Procesamiento igual que antes...
                    id_comment = row['id']
                    text = row['text']
                    
                    published_at_str = row['published_at']
                    if 'T' in published_at_str:
                        published_at = datetime.strptime(published_at_str.split('T')[0], '%Y-%m-%d').date()
                    else:
                        published_at = datetime.strptime(published_at_str, '%Y-%m-%d').date()
                    
                    is_response = False
                    
                    like_count_str = str(row['like_count']).strip()
                    like_count = int(float(like_count_str)) if like_count_str and like_count_str != 'nan' else 0
                    
                    text_lower = str(text).lower() if pd.notna(text) else ""
                    positive_words = ['gracias', 'buen', 'bueno', 'excelente', 'interesante', 'genial', 'perfecto']
                    is_possitive = any(word in text_lower for word in positive_words)
                    
                    formal_indicators = ['saludos', 'estimado', 'cordiales', 'agradecido', 'respecto a']
                    informal_indicators = ['xd', 'lol', 'jaja', 'jajaja', 'bro', 'amigo']
                    
                    is_formal = (any(indicator in text_lower for indicator in formal_indicators) and 
                               not any(indicator in text_lower for indicator in informal_indicators))
                    
                    id_channel = 'default_channel'
                    id_video = row['id_video']
                    
                    registros_lote.append((
                        id_comment, text, published_at, is_response, like_count, 
                        is_possitive, is_formal, id_channel, id_video
                    ))
                    
                except Exception as e:
                    errores += 1
                    print(f"Error procesando fila {index + 1}: {e}")
                    continue
            
            # Insertar el lote completo
            if registros_lote:
                insert_query = """
                INSERT INTO public.comment 
                (id, text, published_at, is_response, like_count, is_possitive, is_formal, id_channel, id_video)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                try:
                    cursor.executemany(insert_query, registros_lote)
                    conn.commit()
                    insertados += len(registros_lote)
                    print(f"Lote insertado: {len(registros_lote)} registros. Total: {insertados}")
                except Exception as e:
                    conn.rollback()
                    errores += len(registros_lote)
                    print(f"Error insertando lote: {e}")
        
        print(f"\nProcesamiento completado:")
        print(f"Total de registros: {total_registros}")
        print(f"Registros insertados: {insertados}")
        print(f"Errores: {errores}")
        
    except Exception as e:
        print(f"Error de conexión a la base de datos: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    # Usa la primera versión para más control individual
    procesar_csv_y_insertar()
    
    # O usa la versión por lotes para mejor rendimiento
    # procesar_csv_por_lotes()