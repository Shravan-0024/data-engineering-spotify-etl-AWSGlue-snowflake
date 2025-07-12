import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime
import urllib.parse
import requests

def lambda_handler(event, context):
    
    cilent_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=cilent_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    access_token = sp.auth_manager.get_access_token(as_dict=False)
    
    search_query = "Top 50 - India"
    q = urllib.parse.quote(search_query)
    url = f"https://api.spotify.com/v1/search?q={q}&type=playlist"

    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    playlist_data = response.json()

    playlist_id = playlist_data['playlists']['items'][5]['id']
    playlist_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"

    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(playlist_url, headers=headers)
    track_data = response.json()

    top_50_songs = track_data['items'][0:50]

    client = boto3.client('s3')
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket="spotify-etl-project-sannu",
        Key="raw_data/to_process/" + filename,
        Body=json.dumps(top_50_songs)
        )

    glue = boto3.client('glue')
    gluejobname = "spotify_transform_s3_load"

    try:
        runId = glue.start_job_run(JobName=gluejobname)
        status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print("Job Status : ", status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)