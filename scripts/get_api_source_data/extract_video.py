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

import json
import pandas as pd
import googleapiclient.discovery
from transformers import pipeline

API_KEY = "AIzaSyBt4M-YrThM36cBR0vm9psJFOl-HY6rizk"
api_service_name = "youtube"
api_version = "v3"


class VideoSummarizer:
    PROMPT = "This are the details of a youtube video, generate a short summary in spanish of max 150 characters:\n\nTitle: {title}\n\nDescription: {description}\n\nSummary:"

    def __init__(self, api_key, api_service_name="youtube", api_version="v3"):
        self.api_key = api_key
        self.api_service_name = api_service_name
        self.api_version = api_version
        self.youtube = googleapiclient.discovery.build(
            self.api_service_name, self.api_version, developerKey=self.api_key
        )
        self.summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

    def get_videos_from_channel(self, channel_name):
        channel_info = CHANNEL_KEYS.get(channel_name)
        if not channel_info:
            print(f"Channel {channel_name} not found in CHANNEL_KEYS.")
            return []

        prefer = channel_info["prefer"]
        fallback = channel_info.get("fallback")

        # Try to get channel ID using preferred method
        request = self.youtube.channels().list(
            part="id",
            forHandle=prefer
        )
        response = request.execute()
        if response["items"]:
            channel_id = response["items"][0]["id"]
        elif fallback:
            # Try fallback method
            request = self.youtube.channels().list(
                part="id",
                forHandle=fallback
            )
            response = request.execute()
            if response["items"]:
                channel_id = response["items"][0]["id"]
            else:
                print(f"Could not find channel ID for {channel_name}.")
                return []
        else:
            print(f"Could not find channel ID for {channel_name}.")
            return []

        # Get videos from the channel
        video_ids = []
        next_page_token = None
        while True:
            request = self.youtube.search().list(
                part="id",
                channelId=channel_id,
                maxResults=50,
                type="video",
                pageToken=next_page_token
            )
            response = request.execute()
            for item in response["items"]:
                video_ids.append(item["id"]["videoId"])
            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        return video_ids

    def filter_videos_by_category(self, video_ids, category_ids):
        filtered_videos = []
        for video_id in video_ids:
            request = self.youtube.videos().list(
                part="snippet",
                id=video_id
            )
            response = request.execute()
            if response["items"]:
                category_id = response["items"][0]["snippet"]["categoryId"]
                if category_id in category_ids:
                    filtered_videos.append(video_id)
        return filtered_videos

    def get_video_details(self, video_ids):
        video_details = []
        for video_id in video_ids:
            request = self.youtube.videos().list(
                part="snippet",
                id=video_id
            )
            response = request.execute()
            if response["items"]:
                snippet = response["items"][0]["snippet"]
                video_details.append({
                    "id": video_id,
                    "title": snippet["title"],
                    "description": snippet["description"]
                })
        return video_details

    def summarize_text(self, title, description):
        text = self.PROMPT.format(title=title, description=description)
        if len(text) > 1024:
            text = text[:1024]
        summary = self.summarizer(text, max_length=150, min_length=30, do_sample=False)
        return summary[0]['summary_text']

    def process(self, channel_name):
        video_ids = self.get_videos_from_channel(channel_name)
        filtered_video_ids = self.filter_videos_by_category(video_ids, ["27", "28", "35"])
        video_details = self.get_video_details(filtered_video_ids)

        video_summaries = []
        for video in video_details:
            summary = self.summarize_text(video["title"], video["description"])
            video_summaries.append({
                "id": video["id"],
                "title": video["title"],
                "description": video["description"],
                "summary": summary
            })
        return video_summaries


if __name__ == "__main__":
    channel_name = "Veritasium"  # Canal a procesar
    summarizer = VideoSummarizer(api_key=API_KEY)
    summaries = summarizer.process(channel_name)

    df = pd.DataFrame(summaries)
    df['video_id'] = df['video_id'].astype(str)

    df_all = pd.read_json("videos_filtrados_por_categoria.json")
    df_all['video_id'] = df_all['video_id'].astype(str)

    df_merge = df_all.join(df.set_index('video_id'), on="video_id", how="left", rsuffix="llm_")
    df_merge = df_merge.drop(columns=['titlellm_', 'descriptionllm_'])

    df_merge["summary"] = df_merge["summary"].fillna("No summary avaliable")
    cols_to_fill = df_merge.columns.difference(["summary"])
    df_merge[cols_to_fill] = df_merge[cols_to_fill].fillna(0)

    df_merge.to_json("videos_filtrados_por_categoria_con_resumen.json", orient="records")
    print("Archivo generado: videos_filtrados_por_categoria_con_resumen.json")