from rdflib.namespace import RDF
import pandas as pd
from rdflib import Graph, Namespace, Literal, URIRef, XSD
import os

# === Prefijos ===
gest = Namespace("http://gestdb.org/")
wd = Namespace("http://www.wikidata.org/entity/")
wdt = Namespace("http://www.wikidata.org/prop/direct/")
schema = Namespace("http://schema.org/")

# === Cargar CSV ===
df = pd.read_csv(os.environ['DATA_PATH'] + "table_comment_data.csv")

# === Crear grafo RDF ===
g = Graph()
g.bind("gest", gest)
g.bind("wd", wd)
g.bind("wdt", wdt)
g.bind("xsd", XSD)
g.bind("schema", schema)

# === Generar tripletas ===
for _, row in df.iterrows():
    s = gest[f"comment_{row['id']}"]

    # tipo de entidad: Comment (Q25345994)
    g.add((s, RDF.type, wd["Q25345994"]))

    # fecha (P577)
    if pd.notna(row.get("published_at")):
        g.add((s, wdt["P577"], Literal(row["published_at"], datatype=XSD.date)))

    # número de likes (P10649)
    if pd.notna(row.get("like_count")):
        g.add((s, wdt["P10649"], Literal(int(row["like_count"]), datatype=XSD.integer)))

    # pertenece a vídeo (P1433)
    if pd.notna(row.get("id_video")):
        g.add((s, wdt["P1433"], gest[f"video_{row['id_video']}"]))

    if pd.notna(row.get("text")):
        # titulo del comentario (texto)
        g.add((s, wdt["P1476"], Literal(row["text"])))

# === Guardar resultado ===
g.serialize("comments.ttl", format="turtle")
