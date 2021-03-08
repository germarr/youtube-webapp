from googleapiclient.discovery import build
import pandas as pd
import json

api_key= ""
youtube = build("youtube","v3", developerKey=api_key)

countries = ["AR","AU","BO","BR","CA","CL","CO","CR","DE","EC","ES","FR","GB","IN","IR","IT","JP","KR","MX","NG","PE","PT","US","UY"]

def all_months():
    empty_list = []
    for a in range(len(countries)):
        for i in range(1,29): 
            if i <= 9:
                i= f"0{i}"
            else:
                i = i
            
            try:
                au_dfOne = pd.read_csv(f"youtube_research//trending_videos_data//{countries[a]}//02//2021_02_{i}//merged_file.csv", index_col=0)
            except OSError:
                au_dfOne= None
            
            empty_list.append(au_dfOne)
    
    return pd.concat(empty_list)
            
a = all_months()
february_mx= a.query('country == "MX"').drop_duplicates(subset=["video_title"])
february_mx["video_trended"]= 1

jb =february_mx.tail(10)["channel_id"]

def getting_data_from_channels(channel_ids):
    empty_list6 = []
    channel_stats = youtube.channels().list(
                part=["statistics","snippet","contentDetails"],
                id=",".join(channel_ids)
                ).execute()
    
    subscriberCount= None
    country = None
    upload_playlist= None

    for i in range(len(channel_stats["items"])):
        try:
            channel_stats["items"][i]["statistics"]["subscriberCount"]
        except KeyError:
            subscriberCount = ""

        if subscriberCount != "":
            subscriberCount = int(channel_stats["items"][i]["statistics"]["subscriberCount"])

        try:
            channel_stats["items"][i]["snippet"]["country"]
        except KeyError:
            country = ""

        if country != "":
            country = channel_stats["items"][i]["snippet"]["country"]

        try:
            channel_stats["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"]
        except KeyError:
            upload_playlist = ""

        if upload_playlist != "":
            upload_playlist= channel_stats["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"]

        empty_list6.append({
            "channel_title" : channel_stats["items"][i]["snippet"]["title"],
            "number_of_views": int(channel_stats["items"][i]["statistics"]["viewCount"]),
            "published_videos": int(channel_stats["items"][i]["statistics"]["videoCount"]),
            "channel_subs":subscriberCount,
            "birth_of_channel" : channel_stats["items"][i]["snippet"]["publishedAt"],
            "country_of_the_channel":country,
            "upload_playlist":upload_playlist,
            "channel_id": channel_stats["items"][i]["id"]
        })
    
    dataframe_output = pd.DataFrame.from_dict(empty_list6)
    return dataframe_output


def list_of_videos(ids):
    first50 = list(ids)
    empty_list = []
    count = len(first50)

    while count > 0:
        var1 = count -50
        var2 = count
    
        if var1 < 0:
            var1=0
        
        empty_list.append(first50[var1:var2])
    
        count -= 50
        
    return empty_list, len(empty_list)


def function80(ids):
    complete_df=[]
    vi, amount = list_of_videos(ids)
    
    for i in range(amount):
        df_v1= getting_data_from_channels(channel_ids=vi[i])
        complete_df.append(df_v1)
    
    return pd.concat(complete_df)

function80(jb)

