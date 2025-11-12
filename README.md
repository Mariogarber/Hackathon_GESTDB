# Hackathon_GESTDB

## Autores

- Mario García Berenguer - mario.gberenguer@alumnos.upm.es
> @Mariogarber
- Eder Tarifa Fernández - eder.tarifa@alumnos.upm.es 
> @EderTarifa
- Pedro Arroyo Urbina - pedro.arroyo@alumnos.upm.es
> @PedroArroyo16
- Pablo Chicharro Gómez - p.cgomez@alumnos.upm.es
> @PabloCG13


## Descripción general

El proyecto **Hackathon_GESTDB** es un prototipo completo de ingeniería de datos que extrae información de varios canales de YouTube educacionales, la almacena en una base de datos relacional, genera representaciones vectoriales (embeddings), analiza el contenido e indexa los datos en **Elasticsearch** y **GraphDB** para hacer consultas avanzadas.  Todo el flujo se orquesta mediante contenedores Docker y se incluyen notebooks con ejemplos de análisis y consultas SPARQL.

El objetivo final es disponer de una base de datos enriquecida con vídeos, canales, comentarios y categorías para realizar búsquedas semánticas y análisis de tendencias, así como visualizar los resultados a través de notebooks y consultas SPARQL.

## Estructura del repositorio

| Carpeta / Archivo | Descripción |
| ----------------- | ----------- |
| `config/` | Contiene ficheros de configuración. El archivo `.env` define las variables de entorno necesarias para conectarse a PostgreSQL (host, nombre de base de datos, usuario y contraseña). |
| `data/` | Almacena los datos de entrada y salidas intermedias. Incluye cuatro subcarpetas:<br> • **`api_data`** → datos brutos obtenidos de la API de YouTube (categorías, canales, vídeos y comentarios).<br> • **`database_data`** → CSV listos para ser insertados en las tablas.<br> • **`embeddings_data`** → archivos CSV con los vectores de comentarios y vídeos.<br> • **`rdf`** → ficheros Turtle con las triples RDF y scripts para generarlos. |
| `database/` | Contiene la definición del esquema de la base de datos en SQL (`database_youtube.sql`) y en formato de modelo (`table_diagram_youtube.dbm`).<br>El script SQL crea las tablas `video`, `category`, `comment` y `channel` con sus claves primarias y foráneas, además de índices para acelerar consultas por fecha y lenguaje. |
| `results/` | Incluye los notebooks (`Results.ipynb`, `sparql_queries.ipynb`) y el archivo `consultas.txt` con consultas SPARQL.<br>Los notebooks ejecutan SQL sobre PostgreSQL para calcular métricas (promedio de duración, likes y comentarios por canal) y analizar correlaciones temporales en los vídeos.<br>Las consultas SPARQL recuperan información adicional de DBpedia/Wikidata (por ejemplo, propiedades de los canales o los vídeos más comentados). |
| `scripts/` | Código Python para cada etapa del pipeline, dividido en cuatro subcarpetas:<br><br>  **`get_api_source_data/`** — Scripts que utilizan la API de YouTube para descargar datos.<br> &nbsp;&nbsp;• `extract_category_info.py`: obtiene las categorías de vídeo y las guarda en JSON.<br> &nbsp;&nbsp;• `channels.py`: recopila información de canales (nombre, idioma, descripción, suscriptores, banners).<br> &nbsp;&nbsp;• `extract_video_info.py`: recupera los vídeos de un canal, filtra por categorías educativas/científicas/documentales, convierte duraciones ISO 8601 a segundos y guarda metadatos.<br> &nbsp;&nbsp;• `extract_video.py`: genera un resumen corto de la descripción con el modelo BART de HuggingFace.<br><br> **`insert_into_postgres/`** — Scripts para cargar los datos en PostgreSQL.<br> &nbsp;&nbsp;• `insert_categories.py`: inserta categorías desde JSON.<br> &nbsp;&nbsp;• `insert_channels.py`: inserta/actualiza canales asegurando la existencia de la tabla.<br> &nbsp;&nbsp;• `insert_videos.py`: normaliza campos y fechas antes de insertar vídeos.<br> &nbsp;&nbsp;• `insert_comments.py`: limpia CSV de comentarios y convierte los likes a enteros antes de insertarlos.<br><br>  **`preprocess/`** — Limpieza y análisis previo.<br> &nbsp;&nbsp;• `preprocessing.py`: limpia resúmenes repetitivos y aplica análisis de sentimiento con el modelo `nlptown/bert-base-multilingual-uncased-sentiment`.<br> &nbsp;&nbsp;• Notebooks `embeddings.ipynb` y `sentiment_analysis_data.ipynb`: generan los CSV de embeddings y resultados de sentimiento.<br><br>  **`from_postgres_to_elastic/`** — Indexación en Elasticsearch.<br> &nbsp;&nbsp;• `insert_into_elasticsearch.py`: conecta PostgreSQL y Elasticsearch, crea índices con mapeos (incluyendo campos `dense_vector` para embeddings) y usa `bulk` para la indexación. |
| `volumenes/` | Directorios vacíos para datos persistentes de los servicios (`elasticsearch_data` y `graphdb_data`). Se montan como volúmenes en los contenedores para conservar índices y repositorios RDF entre ejecuciones. |
| `docker-compose.yml` | Orquesta todos los contenedores necesarios: `postgres` (base de datos), `pgadmin` (interfaz web), `elasticsearch`, `data-importer` (ejecuta los scripts de carga e indexación), `graphdb` (servidor RDF) y `jupyter` (entorno JupyterLab).<br>El servicio `data-importer` instala dependencias, espera a PostgreSQL y Elasticsearch, y ejecuta los scripts de inserción e indexación. |
| `docker_compose_instructions.txt` | Instrucciones básicas para levantar el entorno: ejecutar `docker-compose up -d` y comprobar las tablas con `docker exec -it postgresql psql -U postgres -d bbdd_api_youtube -c "\dt"`. |
| `requirements.txt` | Lista de dependencias Python (`psycopg2-binary`, `pandas`, `numpy`, `elasticsearch`, `jupyterlab`, `python-dotenv`). |
| `request.ipynb` | Notebook de ejemplo (probablemente muestra consultas o peticiones sobre los índices). |
| `README.md` | Documento explicativo del proyecto (este archivo). |


## Esquema de la base de datos

La base de datos relacional se define en `database/database_youtube.sql`.  Las tablas principales son:

- **channel**: almacena los canales (`id`, `name`, `language`, `description`, `suscriber_count`, `banner`, `category_link`).
- **category**: lista de categorías de YouTube (`id`, `name`).
- **video**: guarda los vídeos publicados por los canales (`id`, `title_raw`, `title_processed`, `description`, `published_at`, `language`, `duration`, `view_count`, `like_count`, `thumbnails`, `comment_count`, `topic`, `id_channel`, `id_category`).  Las claves foráneas enlazan con `channel` y `category`.
- **comment**: contiene los comentarios de los vídeos (`id`, `text`, `published_at`, `like_count`, `sentiment_score`, `id_video`).  Referencia a `video` mediante `id_video`.

Se crean índices para acelerar consultas por `published_at` y `language` en las tablas `video`, `channel` y `comment`.

## Flujo de procesamiento

1. **Extracción de datos**:  se ejecutan los scripts de `get_api_source_data` para descargar las categorías, canales, vídeos y comentarios de YouTube mediante la API.  Los datos se guardan en la carpeta `data/api_data` en formato JSON/CSV.

2. **Carga en PostgreSQL**:  mediante el servicio `data-importer` de Docker o ejecutando manualmente los scripts de `insert_into_postgres`, se leen los ficheros de la API y se insertan en las tablas de PostgreSQL.  Cada script maneja la conversión de tipos y la actualización en caso de duplicados.

3. **Preprocesamiento y enriquecimiento**:  usando los notebooks y scripts de `preprocess`, se limpian los textos (títulos y resúmenes), se calcula el sentimiento de los comentarios y se generan embeddings de frases mediante modelos `sentence-transformers`.  Los embeddings se almacenan en `data/embeddings_data` y se usará también `sentiment_score` como campo adicional de la tabla `comment`.

4. **Indexación en Elasticsearch**:  el script `insert_into_elasticsearch.py` lee los datos de PostgreSQL, combina la información con los embeddings y crea índices en Elasticsearch con mapeos específicos para vídeos, comentarios y canales.  Los campos `dense_vector` permiten búsquedas semánticas mediante similitud coseno.

5. **RDF y GraphDB**:  la carpeta `rdf` contiene ficheros `.ttl` que representan la misma información en formato de grafos de conocimiento.  El script `generate_comments.py` convierte el CSV de comentarios en triples RDF usando `rdflib`, añadiendo propiedades como fecha de creación, número de likes y texto del comentario.  El contenedor `graphdb` sirve para cargar estos ficheros y realizar consultas SPARQL.  El archivo `consultas.txt` ofrece ejemplos de consultas para explorar las propiedades de los canales o analizar top vídeos.

6. **Análisis y visualizaciones**:  en la carpeta `results` se incluyen notebooks con consultas SQL sobre PostgreSQL.  Por ejemplo, uno de ellos calcula el promedio de duración, likes y comentarios por canal, y genera gráficos de barras; otro analiza la correlación entre likes y comentarios en periodos vacacionales frente a periodos lectivos; otro agrupa vídeos por franja horaria para estudiar la interacción a lo largo del día.  Los notebooks de `sparql_queries.ipynb` contienen ejemplos de consultas a DBpedia/Wikidata para enriquecer la base de datos con metadatos externos.

## Despliegue y ejecución

1. **Clonar el repositorio** y situarse en la raíz del proyecto.

2. Asegúrate de tener instalados **Docker** y **Docker Compose**.  Crea un archivo `.env` dentro de `config/` siguiendo el ejemplo existente (debe contener las credenciales de PostgreSQL y otros parámetros).  

3. Ejecuta el entorno con:

   ```bash
   docker-compose up -d
   ```

   Esto iniciará los servicios de PostgreSQL, pgAdmin (en `http://localhost:8080`), Elasticsearch (en `http://localhost:9200`), GraphDB (en `http://localhost:7200`), JupyterLab (en `http://localhost:8888`) y un contenedor `data-importer` que realizará automáticamente la carga de datos e indexación.

4. Una vez terminado, puedes acceder a pgAdmin para explorar la base de datos, a Elasticsearch para comprobar que existen los índices `videos`, `comments` y `channels`, y a GraphDB para cargar los ficheros `.ttl` y realizar consultas SPARQL.  Los notebooks de `results` y `preprocess` pueden abrirse con JupyterLab para reproducir los análisis.
   

## Licencia y uso de datos
El código de este repositorio se publica bajo la licencia MIT.

Los datos empleados provienen de la [YouTube Data API v3](https://developers.google.com/youtube/v3) y están sujetos a sus términos de servicio.  
Por tanto, no se redistribuyen los datos brutos obtenidos de YouTube, sino únicamente información derivada (estadísticas agregadas, embeddings, y resultados analíticos).  
El uso de este proyecto implica la aceptación de las políticas de YouTube API Services.

## Contacto

Para dudas o sugerencias sobre el proyecto, puedes crear una issue en GitHub o contactarnos.

