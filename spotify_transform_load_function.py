import json
import boto3
from pprint import pprint
import pandas as pd
from datetime import datetime
from io import StringIO

def extracted_data(data):
    dict1=data
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
      
    return album_list
    





def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-s3bucket-yashreddy'
    Key = 'raw_data/to_be_processed/'
    
    spotify_data=[]
    spotify_key=[]
    
    for i in s3.list_objects(Bucket=Bucket,Prefix=Key)['Contents']:
        file_key = i['Key']
        
        
        
        # checkiing and taking only json files
        if file_key.split('.')[-1]=='json':
            
            print(file_key)
            
            response = s3.get_object(Bucket=Bucket,Key=file_key)
            
            content = response['Body']
            
            jsonObject = json.loads(content.read())
            
            spotify_data.append(jsonObject)
            spotify_key.append(file_key)
            
            k=0
            
            for data in spotify_data:
                
                print("k",k)
                
                albums = []
                singers =[]
                composers=[]
                songs=[]
                
                extract= extracted_data(data)

                for i in extract:
                    albums.append({'album_id':i['album_id'],'album_name':i['album_name'],'album_url':i['album_url'],'album_release_date':i['album_release_date'],
                                    'total_tracks_in_album':i['total_tracks_in_album'],'composer_id':i['composer_id'],'singers_id':[k['singer_id'] for k in i['singers_list'] ],'song_id':i['song_id']
                                    })
                    singers+=[{'singer_id':k['singer_id'],'singer_name':k['singer_name'],'singer_url':k['singer_url']} for k in i['singers_list']]
                    composers+=[{'composer_id':i['composer_id'],'composer_name':i['composer_name'],'composer_url':i['composer_url']}]
                    songs+=[{'song_id':i['song_id'],'song_name':i['song_name'],'song_url':i['song_url']}]
                
                    
                albums_df = pd.DataFrame.from_dict(albums)
                singers_df = pd.DataFrame.from_dict(singers)
                composers_df = pd.DataFrame.from_dict(composers)
                songs_df = pd.DataFrame.from_dict(songs)
                    
                #dropping duplicates
                albums_df.drop_duplicates(subset=['album_id'],inplace=True)
                singers_df.drop_duplicates(subset=['singer_id'],inplace=True)
                composers_df .drop_duplicates(subset=['composer_id'],inplace=True)
                songs_df .drop_duplicates(subset=['song_id'],inplace=True)
                    
                # making datatypes to the way we want
                    
                albums_df['album_release_date']=pd.to_datetime(albums_df['album_release_date'])
            
                song_key = 'transformed_data/songs_data/song_transformed'+str(datetime.now())+'.csv'
                    
                song_buffer = StringIO()
                songs_df.to_csv(song_buffer,index=False)
                song_content = song_buffer.getvalue()
                s3.put_object(Bucket=Bucket,Key=song_key,Body=song_content)
                
                
                singer_key = 'transformed_data/singers_data/singers_transformed'+str(datetime.now())+'.csv'
                    
                singer_buffer = StringIO()
                singers_df.to_csv(singer_buffer,index=False)
                singer_content = singer_buffer.getvalue()
                s3.put_object(Bucket=Bucket,Key=singer_key,Body=singer_content)
                
                
                album_key = 'transformed_data/albums_data/albums_transformed'+str(datetime.now())+'.csv'
                    
                album_buffer = StringIO()
                albums_df.to_csv(album_buffer,index=False)
                album_content = album_buffer.getvalue()
                s3.put_object(Bucket=Bucket,Key=album_key,Body=album_content)
                
                
            
                composer_key = 'transformed_data/composers_data/composers_transformed'+str(datetime.now())+'.csv'
                    
                composer_buffer = StringIO()
                composers_df.to_csv(composer_buffer,index=False)
                composer_content = composer_buffer.getvalue()
                s3.put_object(Bucket=Bucket,Key=composer_key,Body=composer_content)
                
                
                
            # now copying files to processed and deleting from to_processed
                
            s3_resource = boto3.resource('s3')
                
            for key in spotify_key:
                    
                copy_source={
                    'Bucket':Bucket,
                    'Key':key
                }
                    
                s3_resource.meta.client.copy(copy_source,Bucket,'raw_data/processed/'+ key.split('/')[-1])
                s3_resource.Object(Bucket,key).delete()