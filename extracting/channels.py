import json
import googleapiclient.discovery

# Configuración de la API
API_KEY = "AIzaSyALKa_PvOxGQDRFduccOGrKK2njCk_korY"
api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=API_KEY)

def get_channel_info(channel_key: str):
    """
    channel_key puede ser un @handle o un username legado.
    Devuelve dict con info del canal (o None si no se encuentra).
    """
    try:
        if channel_key.startswith("@"):
            resp = youtube.channels().list(part="id", forHandle=channel_key[1:]).execute()
        else:
            resp = youtube.channels().list(part="id", forUsername=channel_key).execute()

        if not resp.get("items"):
            # Fallback: búsqueda por nombre/handle
            sr = youtube.search().list(part="id", q=channel_key, type="channel", maxResults=1).execute()
            if not sr.get("items"):
                print(f"No se encontró el canal: {channel_key}")
                return None
            channel_id = sr["items"][0]["id"]["channelId"]
        else:
            channel_id = resp["items"][0]["id"]

        full = youtube.channels().list(
            part="snippet,statistics,brandingSettings,contentDetails,topicDetails",
            id=channel_id
        ).execute()

        if not full.get("items"):
            return None

        ch = full["items"][0]
        sn = ch.get("snippet", {})
        st = ch.get("statistics", {})

        return {
            "id": channel_id,
            "name": sn.get("title", ""),
            "language": sn.get("defaultLanguage", ""),
            "description": sn.get("description", ""),
            "subscriber_count": int(st.get("subscriberCount", 0)) if st.get("subscriberCount") else 0,
            "banner": sn.get("thumbnails", {}).get("high", {}).get("url", ""),
            "custom_url": sn.get("customUrl", ""),
            "handle": sn.get("customUrl", ""),  # muchas veces coincide con @handle
        }
    except Exception as e:
        print(f"Error al obtener datos del canal {channel_key}: {e}")
        return None

# Diccionario con canales a analizar
CHANNEL_KEYS = {
    "QuantumFracture":        {"prefer": "QuantumFracture",   "fallback": "@QuantumFracture"},
    "Nate Gentile":           {"prefer": "@NateGentile7"},
    "Mark Rober":             {"prefer": "onemeeeliondollars", "fallback": "@MarkRober"},
    "ElectroBOOM":            {"prefer": "msadaghd",          "fallback": "@ElectroBOOM"},
    "Dr.Gajendra Purohit":    {"prefer": "@gajendrapurohit"},
    "Traductor de Ingeniería":{"prefer": "@eltraductor_ok"},
    "3Blue1Brown":            {"prefer": "@3blue1brown"},
    "A toda leche":           {"prefer": "Lechero",           "fallback": "@Atodaleche"},
    "Memorias de Pez":        {"prefer": "@MemoriasDePez"},
    "MoureDev by Brais Moure":{"prefer": "@mouredev"},
    "Veritasium":             {"prefer": "1veritasium",       "fallback": "@veritasium"},
    "Mathologer":             {"prefer": "@Mathologer"},
    "BBC Timestamp":          {"prefer": "@BBCTimestamp"},
}

# Procesar los canales
canales_info = {}
for display_name, keys in CHANNEL_KEYS.items():
    key = keys["prefer"]
    info = get_channel_info(key)
    if info is None and "fallback" in keys:
        info = get_channel_info(keys["fallback"])
    canales_info[display_name] = info

# Guardar en un archivo JSON
with open("canales_youtube_info.json", "w", encoding="utf-8") as f:
    json.dump(canales_info, f, ensure_ascii=False, indent=4)

print("Archivo JSON generado: canales_youtube_info.json")
