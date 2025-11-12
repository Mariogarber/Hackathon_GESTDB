import googleapiclient.discovery
import json
import isodate
import time

API_KEY = "AIzaSyALKa_PvOxGQDRFduccOGrKK2njCk_korY"
api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=API_KEY)

# Obtener mapa de categorías: id -> nombre
def get_category_map(region_code="ES"):
    category_map = {}
    try:
        response = youtube.videoCategories().list(part="snippet", regionCode=region_code).execute()
        for item in response.get("items", []):
            category_map[item["id"]] = item["snippet"]["title"]
    except Exception as e:
        print("Error al obtener categorías:", e)
    return category_map

# Convertir duración ISO 8601 a segundos
def iso8601_duration_to_seconds(duration_iso):
    try:
        duration = isodate.parse_duration(duration_iso)
        return int(duration.total_seconds())
    except Exception:
        return None

# Obtener ID de playlist de subidas del canal
def get_channel_uploads_playlist_id(channel_id: str):
    response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

    items = response.get("items", [])
    if not items:
        print("No se encontró el canal.")
        return None

    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

# Obtener todos los vídeos filtrados por categoría
def get_videos_from_channel(channel_id: str):
    uploads_playlist_id = get_channel_uploads_playlist_id(channel_id)
    if not uploads_playlist_id:
        return []

    category_map = get_category_map()
    allowed_categories = {"27", "28", "35"}  # Educación, Ciencia y tecnología, Documentales

    videos = []
    next_page_token = None

    while True:
        playlist_response = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        video_ids = [item["contentDetails"]["videoId"] for item in playlist_response["items"]]

        video_response = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(video_ids)
        ).execute()

        for item in video_response["items"]:
            snippet = item["snippet"]
            stats = item.get("statistics", {})
            details = item.get("contentDetails", {})

            category_id = snippet.get("categoryId", "")
            if category_id not in allowed_categories:
                continue  # Filtrar por categoría

            duration_iso = details.get("duration", "")
            duration_seconds = iso8601_duration_to_seconds(duration_iso)

            videos.append({
                "id_channel": channel_id,
                "video_id": item["id"],
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "language": snippet.get("defaultAudioLanguage", snippet.get("defaultLanguage", "")),
                "published_at": snippet.get("publishedAt", ""),
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)) if "likeCount" in stats else None,
                "comment_count": int(stats.get("commentCount", 0)) if "commentCount" in stats else None,
                "duration": duration_seconds,
                "thumbnails": snippet.get("thumbnails", {}),
                "tags": snippet.get("tags", []),
                "video_category_id": category_id,
                "video_category": category_map.get(category_id, "Desconocido")
            })

        next_page_token = playlist_response.get("nextPageToken")
        if not next_page_token:
            break

        time.sleep(1)  # evitar cuota excesiva

    return videos

# Uso del script
channel_id = "UCHnyfMqiRRG1u-2MsSQLbXA"  # Reemplaza con el canal que desees
videos_info = get_videos_from_channel(channel_id)

# Guardar en JSON
with open("videos_filtrados_por_categoria.json", "w", encoding="utf-8") as f:
    json.dump(videos_info, f, ensure_ascii=False, indent=4)

print(f"Se han guardado {len(videos_info)} vídeos filtrados en 'videos_filtrados_por_categoria.json'")
