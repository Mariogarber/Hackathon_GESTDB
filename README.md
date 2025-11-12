# Hackathon_GESTDB

## Autores

- Mario García Berenguer - mario.gberenguer@alumnos.upm.es
> @Mariogarber
- Eder
> @
- Pedro
> @
- Pablo
> @


## Descripción general

El proyecto **Hackathon_GESTDB** es un prototipo completo de ingeniería de datos que extrae información de varios canales de YouTube educacionales, la almacena en una base de datos relacional, genera representaciones vectoriales (embeddings), analiza el contenido e indexa los datos en **Elasticsearch** y **GraphDB** para hacer consultas avanzadas.  Todo el flujo se orquesta mediante contenedores Docker y se incluyen notebooks con ejemplos de análisis y consultas SPARQL.

El objetivo final es disponer de una base de datos enriquecida con vídeos, canales, comentarios y categorías para realizar búsquedas semánticas y análisis de tendencias, así como visualizar los resultados a través de notebooks y consultas SPARQL.

## Estructura del repositorio

| Carpeta / Archivo | Descripción |
| ----------------- | ----------- |
| `config/` | Contiene ficheros de configuración. El archivo `.env` define las variables de entorno necesarias para conectarse a PostgreSQL (host, nombre de base de datos, usuario y contraseña)【413150139228417†screenshot】. |
| `data/` | Almacena los datos de entrada y salidas intermedias. Incluye cuatro subcarpetas: **`api_data`** (datos brutos obtenidos de la API de YouTube: categorías, canales, vídeos y comentarios), **`database_data`** (CSV listos para ser insertados en las tablas), **`embeddings_data`** (archivos CSV con los vectores de comentarios y vídeos) y **`rdf`** (ficheros Turtle con las triples RDF y scripts para generarlos). |
| `database/` | Contiene la definición del esquema de la base de datos en SQL (`database_youtube.sql`) y en formato de modelo (`table_diagram_youtube.dbm`).  El script SQL crea las tablas `video`, `category`, `comment` y `channel` con sus claves primarias y foráneas【643779282038525†L14-L38】【643779282038525†L83-L138】.  También define índices para acelerar consultas por fecha y lenguaje. |
| `results/` | Aquí se guardan los notebooks (`Results.ipynb`, `sparql_queries.ipynb`) y el archivo `consultas.txt` con consultas SPARQL.  Los notebooks ejecutan SQL sobre PostgreSQL para calcular métricas como el promedio de duración, likes y comentarios por canal y analizan correlaciones temporales en los vídeos.  Las consultas SPARQL recuperan información adicional de DBpedia/Wikidata (por ejemplo, propiedades de los canales o los vídeos más comentados)【446720107742744†screenshot】【112069695352358†screenshot】. |
| `scripts/` | Código Python para cada etapa del pipeline.  Está dividido en cuatro subcarpetas: 
  * **`get_api_source_data`**: scripts que utilizan la API de YouTube para descargar datos.  `extract_category_info.py` obtiene todas las categorías de vídeo y las guarda en JSON【106777078423534†L10-L24】; `channels.py` recopila información de canales (nombre, idioma, descripción, suscriptores, banners) a partir de sus identificadores o `@handles`【757936036714175†L11-L60】; `extract_video_info.py` recupera los vídeos de un canal, filtra por categorías educativas, científicas y documentales, convierte duraciones ISO 8601 a segundos y almacena metadatos (vistas, likes, comentarios, idioma, duración, miniaturas)【215516357507780†L46-L104】; `extract_video.py` utiliza un modelo BART para generar un resumen corto en español de la descripción de cada vídeo【882158511312112†L31-L154】.  
  * **`insert_into_postgres`**: scripts para cargar los datos en PostgreSQL.  `insert_categories.py` lee el JSON de categorías y lo inserta en la tabla `category` (actualizando en caso de conflicto)【328312397630582†L21-L63】; `insert_channels.py` lee el JSON de canales, espera a que la base de datos esté disponible, comprueba que existe la tabla `channel` y luego inserta/actualiza cada canal【519809219162134†L67-L129】; `insert_videos.py` procesa cada vídeo (convirtiendo fechas y duraciones, normalizando campos) e inserta en la tabla `video`【621564881610690†L31-L100】; `insert_comments.py` carga el CSV de comentarios con pandas, limpia las fechas y convierte los likes a enteros antes de insertarlos en la tabla `comment`【504022493419267†L32-L93】.  
  * **`preprocess`**: contiene notebooks y scripts de limpieza y análisis previo.  El script `preprocessing.py` incluye funciones para limpiar resúmenes (eliminando frases repetitivas) y aplicar análisis de sentimiento a los comentarios utilizando el modelo multilingüe `nlptown/bert-base-multilingual-uncased-sentiment`【234227705079486†L13-L24】.  Los notebooks `embeddings.ipynb` y `sentiment_analysis_data.ipynb` calculan embeddings de textos y muestran resultados de sentimiento, generando los archivos CSV de la carpeta `embeddings_data`.  
  * **`from_postgres_to_elastic`**: contiene el script `insert_into_elasticsearch.py`, el cual se conecta a PostgreSQL y a Elasticsearch, crea los índices con mapeos para vídeos, comentarios y canales (incluyendo campos `dense_vector` para embeddings)【134808238995968†L83-L104】【134808238995968†L109-L139】, lee las tablas y archivos de embeddings, prepara documentos y usa el helper `bulk` para indexarlos en Elasticsearch. |
| `volumenes/` | Directorios vacíos para almacenar datos persistentes de los servicios: `elasticsearch_data` y `graphdb_data`【597843836575497†L0-L44】.  Se montan como volúmenes en los contenedores para que los índices y repositorios RDF se conserven entre ejecuciones. |
| `docker-compose.yml` | Orquesta todos los contenedores necesarios: `postgres` (con la base de datos y carga inicial del esquema), `pgadmin` (interfaz web para PostgreSQL), `elasticsearch`, `data-importer` (ejecuta los scripts de carga de datos y de indexación), `graphdb` (servidor RDF), y `jupyter` (JupyterLab con las dependencias instaladas)【364216263720675†L0-L108】.  El servicio `data-importer` instala las dependencias, espera a que PostgreSQL y Elasticsearch estén listos y ejecuta secuencialmente los scripts de inserción y indexación【364216263720675†L60-L77】. |
| `docker_compose_instructions.txt` | Instrucciones básicas para levantar el entorno: ejecutar `docker-compose up -d` y comprobar las tablas con `docker exec -it postgresql psql -U postgres -d bbdd_api_youtube -c "\dt"`【147104557622046†L0-L7】. |
| `requirements.txt` | Lista de dependencias Python necesarias (`psycopg2-binary`, `pandas`, `numpy`, `elasticsearch`, `jupyterlab`, `python-dotenv`)【52320814873479†L0-L4】. |
| `request.ipynb` | Notebook de ejemplo (no explorado en detalle) que probablemente muestra cómo consultar la API o realizar peticiones de búsqueda sobre los índices. |
| `README.md` (este archivo) | Documento explicativo del proyecto.

## Esquema de la base de datos

La base de datos relacional se define en `database/database_youtube.sql`.  Las tablas principales son:

- **channel**: almacena los canales (`id`, `name`, `language`, `description`, `suscriber_count`, `banner`, `category_link`)【643779282038525†L66-L76】.
- **category**: lista de categorías de YouTube (`id`, `name`)【643779282038525†L38-L45】.
- **video**: guarda los vídeos publicados por los canales (`id`, `title_raw`, `title_processed`, `description`, `published_at`, `language`, `duration`, `view_count`, `like_count`, `thumbnails`, `comment_count`, `topic`, `id_channel`, `id_category`)【643779282038525†L14-L31】.  Las claves foráneas enlazan con `channel` y `category`【643779282038525†L83-L138】.
- **comment**: contiene los comentarios de los vídeos (`id`, `text`, `published_at`, `like_count`, `sentiment_score`, `id_video`)【643779282038525†L50-L59】.  Referencia a `video` mediante `id_video`【643779282038525†L126-L131】.

Se crean índices para acelerar consultas por `published_at` y `language` en las tablas `video`, `channel` y `comment`【643779282038525†L90-L123】.

## Flujo de procesamiento

1. **Extracción de datos**:  se ejecutan los scripts de `get_api_source_data` para descargar las categorías, canales, vídeos y comentarios de YouTube mediante la API.  Los datos se guardan en la carpeta `data/api_data` en formato JSON/CSV.

2. **Carga en PostgreSQL**:  mediante el servicio `data-importer` de Docker o ejecutando manualmente los scripts de `insert_into_postgres`, se leen los ficheros de la API y se insertan en las tablas de PostgreSQL.  Cada script maneja la conversión de tipos y la actualización en caso de duplicados.

3. **Preprocesamiento y enriquecimiento**:  usando los notebooks y scripts de `preprocess`, se limpian los textos (títulos y resúmenes), se calcula el sentimiento de los comentarios y se generan embeddings de frases mediante modelos `sentence-transformers`.  Los embeddings se almacenan en `data/embeddings_data` y se usará también `sentiment_score` como campo adicional de la tabla `comment`.

4. **Indexación en Elasticsearch**:  el script `insert_into_elasticsearch.py` lee los datos de PostgreSQL, combina la información con los embeddings y crea índices en Elasticsearch con mapeos específicos para vídeos, comentarios y canales.  Los campos `dense_vector` permiten búsquedas semánticas mediante similitud coseno【134808238995968†L83-L104】【134808238995968†L109-L139】.

5. **RDF y GraphDB**:  la carpeta `rdf` contiene ficheros `.ttl` que representan la misma información en formato de grafos de conocimiento.  El script `generate_comments.py` convierte el CSV de comentarios en triples RDF usando `rdflib`, añadiendo propiedades como fecha de creación, número de likes y texto del comentario【397339552200192†screenshot】.  El contenedor `graphdb` sirve para cargar estos ficheros y realizar consultas SPARQL.  El archivo `consultas.txt` ofrece ejemplos de consultas para explorar las propiedades de los canales o analizar top vídeos【446720107742744†screenshot】.

6. **Análisis y visualizaciones**:  en la carpeta `results` se incluyen notebooks con consultas SQL sobre PostgreSQL.  Por ejemplo, uno de ellos calcula el promedio de duración, likes y comentarios por canal, y genera gráficos de barras; otro analiza la correlación entre likes y comentarios en periodos vacacionales frente a periodos lectivos; otro agrupa vídeos por franja horaria para estudiar la interacción a lo largo del día【839885288441041†L1318-L1373】【839885288441041†L2075-L2145】【839885288441041†L2278-L2319】.  Los notebooks de `sparql_queries.ipynb` contienen ejemplos de consultas a DBpedia/Wikidata para enriquecer la base de datos con metadatos externos.

## Despliegue y ejecución

1. **Clonar el repositorio** y situarse en la raíz del proyecto.

2. Asegúrate de tener instalados **Docker** y **Docker Compose**.  Crea un archivo `.env` dentro de `config/` siguiendo el ejemplo existente (debe contener las credenciales de PostgreSQL y otros parámetros).  

3. Ejecuta el entorno con:

   ```bash
   docker-compose up -d
   ```

   Esto iniciará los servicios de PostgreSQL, pgAdmin (en `http://localhost:8080`), Elasticsearch (en `http://localhost:9200`), GraphDB (en `http://localhost:7200`), JupyterLab (en `http://localhost:8888`) y un contenedor `data-importer` que realizará automáticamente la carga de datos e indexación【364216263720675†L60-L77】.

4. Verifica que las tablas se han creado correctamente ejecutando:

   ```bash
   docker exec -it postgresql psql -U postgres -d bbdd_api_youtube -c "\dt"
   ```【147104557622046†L0-L7】

5. Una vez terminado, puedes acceder a pgAdmin para explorar la base de datos, a Elasticsearch para comprobar que existen los índices `videos`, `comments` y `channels`, y a GraphDB para cargar los ficheros `.ttl` y realizar consultas SPARQL.  Los notebooks de `results` y `preprocess` pueden abrirse con JupyterLab para reproducir los análisis.

## Licencia y uso de datos
El código de este repositorio se publica bajo la licencia MIT.

Los datos empleados provienen de la [YouTube Data API v3](https://developers.google.com/youtube/v3) y están sujetos a sus términos de servicio.  
Por tanto, no se redistribuyen los datos brutos obtenidos de YouTube, sino únicamente información derivada (estadísticas agregadas, embeddings, y resultados analíticos).  
El uso de este proyecto implica la aceptación de las políticas de YouTube API Services.

## Contacto

Para dudas o sugerencias sobre el proyecto, puedes crear una issue en GitHub o contactarnos.

