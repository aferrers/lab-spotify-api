# 1 create function to seach for song:
import pandas as pd
import numpy as np
import requests
from typing import Optional
import spotipy
from spotipy import Spotify

def search_song(title: str, artist: str, sp: spotipy.Spotify = None) -> Optional[str]:
    '''
    This function uses Spotify's API to find a song ID based on its title and artist.
    inputs:
       title (str): The title of the song.
       artist (str): The artist of the song.
       
    outputs:
       Optional[str]: The ID of the song if it exists, otherwise None.
    '''
    if sp is None:
        raise ValueError("Please check Spotify credentials/object.")
    
    results = sp.search(q=f'track:{title} artist:{artist}', type='track')
    items = results['tracks']['items']

    if len(items) > 0:
        song = items[0]
        song_id=(song['id'])
    else:
        song_id = "Song not found"
    return song_id





# create function for creating chunks
def split_into_chunks(df:pd.DataFrame, rows_per_chunk:int=25) -> list[pd.DataFrame]:
    '''
    create chunks based on parameters
    input: (df:pd.DataFrame, rows_per_chunk:int=25)
    output: list of dataframes
    '''
    num_chunks = len(df) // rows_per_chunk + (len(df) % rows_per_chunk > 0) #calculate number of chunks needed
    chunks = np.array_split(df, num_chunks) #split data into chunks
    return chunks





# 2 create list of ids
import time

def list_of_song_ids(df:pd.DataFrame, sp: spotipy.Spotify = None) -> list:
    '''
    This function takes a DataFrame as input, splits it into chunks of 25 rows each, and processes each chunk separately. 
    For each row in a chunk, it tries to find the song ID using the song title and artist. 
    If successful, it appends the song ID to a list; otherwise, it appends an empty string. 
    After processing each chunk, it waits for 25 seconds before moving to the next chunk. 
    Finally, it returns the list of song IDs.
    '''
    list_of_ids = [] #create empty list
    chunks = split_into_chunks(df, 25) #using defined function to create chunks
    for chunk_df in chunks:
        for index, row in chunk_df.iterrows():
            try:
                song_id = search_song(row['Song_title'], row['Artist'], sp)         
                print(f"Song Title: {row['Song_title']}, Artist: {row['Artist']}, Song ID: {song_id}")
                list_of_ids.append(song_id)
            except:
                print("Song not found")
                list_of_ids.append("")
        print("sleep a bit before getting the next chunk")  
        time.sleep(25)
    return list_of_ids






# 3 Function: Get the audio features of a song
from typing import List, Optional
import pandas as pd

def get_audio_features(df:pd.DataFrame, id_column:str='id', sp: spotipy.Spotify = None) -> Optional[pd.DataFrame]:

    '''
    This function uses Spotify's API to get the audio features of songs based on their IDs.
    inputs:
    df (pd.DataFrame): A DataFrame containing song IDs.
    id_column (str): The name of the column in the DataFrame that contains the song IDs.
    outputs:
    pd.DataFrame: A DataFrame containing the audio features of the songs. If a request fails, the corresponding row will contain NaN values.
    '''
    
    if sp is None:
      raise ValueError("Please check Spotify credentials/object.")
        
    chunks = split_into_chunks(df, 20) #using defined function to create chunks
    all_audio_features = []
    
    for chunk_df in chunks:
        try:
            audio_features = sp.audio_features(chunk_df[id_column])
            audio_features_df = pd.DataFrame(audio_features)
            print(audio_features_df)
            all_audio_features.append(audio_features_df)
        except Exception as e:
            print(f"An error occurred: {e}")
            all_audio_features.append(pd.DataFrame(np.nan, index=range(len(chunk_df)), columns=['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'type', 'id', 'uri', 'track_href', 'analysis_url']))
        print("sleep a bit before getting the next chunk")  
        time.sleep(25)
        
    merged_features = pd.concat(all_audio_features, ignore_index=True)
    return merged_features





# 6 create function to concatenate id dataframe with audio features

def add_audio_features(df_ids: pd.DataFrame, df_audio_features: pd.DataFrame) -> pd.DataFrame:
   """
   Concatenates two dataframes.

   Inputs:
   df_ids (pd.DataFrame): The first dataframe with ids.
   df_audio_features (pd.DataFrame): The second dataframe with audio features.

   Output:
   pd.DataFrame: The concatenated dataframe.
   """
   return pd.concat([df_ids, df_audio_features], axis=1)




