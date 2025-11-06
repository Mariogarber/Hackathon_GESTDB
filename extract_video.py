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
import googleapiclient.discovery
from transformers import pipeline

API_KEY = "AIzaSyBt4M-YrThM36cBR0vm9psJFOl-HY6rizk"
api_service_name = "youtube"
api_version = "v3"

PROMPT = "This are the details of a youtube video, generate a short summary in spanish of max 150 characters:\n\nTitle: {title}\n\nDescription: {description}\n\nSummary:"

# define a function that given a channel name returns an a array of videos with their ids

def get_videos_from_channel(channel_name):
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=API_KEY)

    channel_info = CHANNEL_KEYS.get(channel_name)
    if not channel_info:
        print(f"Channel {channel_name} not found in CHANNEL_KEYS.")
        return []

    prefer = channel_info["prefer"]
    fallback = channel_info.get("fallback")

    # Try to get channel ID using preferred method
    request = youtube.channels().list(
        part="id",
        forUsername=prefer
    )
    response = request.execute()
    if response["items"]:
        channel_id = response["items"][0]["id"]
    elif fallback:
        # Try fallback method
        request = youtube.channels().list(
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
        request = youtube.search().list(
            part="id",
            channelId=channel_id,
            maxResults=50,
            type="video",
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response["items"]:
            #filter by category id 27, 28, 35
            video_ids.append(item["id"]["videoId"])
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return video_ids

# make a function that filter an array of videos by category id
def filter_videos_by_category(video_ids, category_ids):
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=API_KEY)

    filtered_videos = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet",
            id=video_id
        )
        response = request.execute()
        if response["items"]:
            category_id = response["items"][0]["snippet"]["categoryId"]
            if category_id in category_ids:
                filtered_videos.append(video_id)
    return filtered_videos

# make a function that given a list of videos ids returns their titles and descriptions
def get_video_details(video_ids):
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=API_KEY)

    video_details = []
    for video_id in video_ids:
        request = youtube.videos().list(
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

# make a function that given a title and description returns a summary using a transformer model
def summarize_text(title, description):

    summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

    text = PROMPT.format(title=title, description=description)
    # limit to 1024 tokens
    if len(text) > 1024:
        text = text[:1024]

    summary = summarizer(text, max_length=150, min_length=30, do_sample=False)
    return summary[0]['summary_text']

# define a function that given a channel name returns an array of video summaries in a dictionary
def get_video_summaries_from_channel(channel_name):
    video_ids = get_videos_from_channel(channel_name)
    # filter by category ids 27 (Education), 28 (Science & Technology), 35 (Comedy)
    filtered_video_ids = filter_videos_by_category(video_ids, ["27", "28", "35"])
    video_details = get_video_details(filtered_video_ids)

    video_summaries = []
    for video in video_details:
        summary = summarize_text(video["title"], video["description"])
        video_summaries.append({
            "id": video["id"],
            "title": video["title"],
            "description": video["description"],
            "summary": summary
        })
    return video_summaries