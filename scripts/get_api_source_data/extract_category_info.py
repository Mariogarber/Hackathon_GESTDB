import googleapiclient.discovery
import json

API_KEY = "AIzaSyALKa_PvOxGQDRFduccOGrKK2njCk_korY"
api_service_name = "youtube"
api_version = "v3"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=API_KEY)

def get_all_video_categories(region_code="ES"):
    try:
        response = youtube.videoCategories().list(
            part="snippet",
            regionCode=region_code
        ).execute()

        categorias = []
        for item in response.get("items", []):
            categorias.append({
                "id": item["id"],
                "title": item["snippet"].get("title", ""),
                "assignable": item["snippet"].get("assignable", False),
                "channelId": item["snippet"].get("channelId", "")
            })

        return categorias

    except Exception as e:
        print("Error al obtener categorías:", e)
        return []

# Obtener y guardar categorías
categorias = get_all_video_categories(region_code="ES")

with open("categorias_video_youtube.json", "w", encoding="utf-8") as f:
    json.dump(categorias, f, ensure_ascii=False, indent=4)

print(f"Se han guardado {len(categorias)} categorías en 'categorias_video_youtube.json'")
