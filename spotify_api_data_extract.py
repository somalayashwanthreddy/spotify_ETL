import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    
    client = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client)
    
    playlist_link='https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF'

    playlist_uri=playlist_link.split('/')[-1]
    
    spotify_data=sp.playlist(playlist_uri)
    
    file_name = "spotify_raw_"+str(datetime.now())+".json"
    
    client=boto3.client('s3')
    client.put_object(
        Body=json.dumps(spotify_data),
        Bucket='spotify-s3bucket-yashreddy',
        Key='raw_data/to_be_processed/'+file_name
        
        )

