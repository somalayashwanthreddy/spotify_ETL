# -*- coding: utf-8 -*-
"""Untitled

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1osKMHhS2-wzxta4YQktPNicnqWfwEFHv
"""

!pip install spotipy

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from pprint import pprint

#here place your credentials
client = SpotifyClientCredentials(,)
sp = spotipy.Spotify(client_credentials_manager=client)

playlist_link='https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF'

playlist_uri=playlist_link.split('/')[-1]
dict1=sp.playlist(playlist_uri)

pprint(dict1['tracks']['items'][0]['track']['id'])

album_list=[]
for i in dict1['tracks']['items']:
  album_name=i['track']['album']['name']
  album_id = i['track']['album']['id']
  composer_id=i['track']['album']['artists'][0]['id']
  composer_name=i['track']['album']['artists'][0]['name']
  composer_url=i['track']['album']['artists'][0]['external_urls']['spotify']
  album_url = i['track']['album']['external_urls']['spotify']
  total_tracks_in_album=i['track']['album']['total_tracks']
  album_release_date= i['track']['album']['release_date']
  singers_list=[]
  for singers in i['track']['artists']:
    singer_name=singers['name']
    singer_url=singers['external_urls']['spotify']
    singer_id = singers['id']
    singers_list.append({'singer_name':singer_name,
              'singer_id':singer_id,
              'singer_url':singer_url,})
  song_id = i['track']['id']
  song_name = i['track']['name']
  song_url= i['track']['external_urls']['spotify']

  album_dict={'album_name':album_name,
              'album_id':album_id,
              'album_url':album_url,
              'album_release_date':album_release_date,
              'total_tracks_in_album':total_tracks_in_album,
              'composer_name':composer_name,
              'composer_id':composer_id,
              'composer_url':composer_url,
              'singers_list':singers_list,
              'song_id':song_id,
              'song_name':song_name,
              'song_url':song_url}

  album_list.append(album_dict)

albums = []
singers =[]
composers=[]
songs=[]
for i in album_list:
  albums.append({'album_id':i['album_id'],'album_name':i['album_name'],'album_url':i['album_url'],'album_release_date':i['album_release_date'],
                 'total_tracks_in_album':i['total_tracks_in_album'],'composer_id':i['composer_id'],'singers_id':[k['singer_id'] for k in i['singers_list'] ],'song_id':i['song_id']
                 })
  singers+=[{'singer_id':k['singer_id'],'singer_name':k['singer_name'],'singer_url':k['singer_url']} for k in i['singers_list']]
  composers+=[{'composer_id':i['composer_id'],'composer_name':i['composer_name'],'composer_url':i['composer_url']}]
  songs+=[{'song_id':i['song_id'],'song_name':i['song_name'],'song_url':i['song_url']}]

albums_df = pd.DataFrame.from_dict(albums)
albums_df.head(5)

singers_df = pd.DataFrame.from_dict(singers)
singers_df.head(5)

composers_df = pd.DataFrame.from_dict(composers)
composers_df.head(5)

songs_df = pd.DataFrame.from_dict(songs)
songs_df .head(5)

#dropping duplicates
albums_df.drop_duplicates(subset=['album_id'],inplace=True)
singers_df.drop_duplicates(subset=['singer_id'],inplace=True)
composers_df .drop_duplicates(subset=['composer_id'],inplace=True)
songs_df .drop_duplicates(subset=['song_id'],inplace=True)

albums_df.info()

albums_df['album_release_date']=pd.to_datetime(albums_df['album_release_date'])
