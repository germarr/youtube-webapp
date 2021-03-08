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
                au_dfOne = pd.read_csv(f"..//youtube_research//trending_videos_data//{countries[a]}//02//2021_02_{i}//merged_file.csv", index_col=0)
            except OSError:
                au_dfOne= None
            
            empty_list.append(au_dfOne)
    
    return pd.concat(empty_list)
            
a = all_months()
february_mx= a.query('country == "MX"').drop_duplicates(subset=["video_title"])
february_mx["video_trended"]= 1

test =february_mx[["channel_title","channel_id","published_date","trending_date","video_title","category_title","views","likes","dislikes","comments","description","link","thumbnail","video_lang","country","duration"]].sort_values(by="channel_title", ascending=True)
runn_this= test[["channel_title"]].groupby("channel_title").size().tolist()

list_empty_II = []
accounting = 0

for am in range(len(runn_this)):
    for si in range(int(runn_this[am])):
        list_empty_II.append({
            "channel_title": test.iloc[accounting].tolist()[0],
            "channel_id": test.iloc[accounting].tolist()[1],
            f"published_date{si}":test.iloc[accounting].tolist()[2],
            f"trending_date{si}":test.iloc[accounting].tolist()[3],
            f"video_title{si}": test.iloc[accounting].tolist()[4],
            f"category_title{si}":test.iloc[accounting].tolist()[5],
            f"views{si}":test.iloc[accounting].tolist()[6],
            f"likes{si}":test.iloc[accounting].tolist()[7],
            f"dislikes{si}":test.iloc[accounting].tolist()[8],
            f"comments{si}":test.iloc[accounting].tolist()[9],
            f"description{si}":test.iloc[accounting].tolist()[10],
            f"link{si}":test.iloc[accounting].tolist()[11],
            f"thumbnail{si}":test.iloc[accounting].tolist()[12],
            f"video_lang{si}":test.iloc[accounting].tolist()[13],
            f"country{si}":test.iloc[accounting].tolist()[14],
            f"duration{si}":test.iloc[accounting].tolist()[15],
        })
        accounting += 1

helloV1= pd.DataFrame.from_dict(list_empty_II).groupby("channel_title").first().reset_index().set_index("channel_title").transpose().fillna(0)

empty_list_11 = []

for i in range(20):
    empty_list_11.append({
            "channel": helloV1.columns[i],
            "id": helloV1.iloc[:,i].to_frame().loc[f"channel_id"][0],
            "items":[]
        })
    for si in range(runn_this[i]):
        empty_list_11[i]["items"].append(
            {
                "item":si,
                "published_date":helloV1.iloc[:,i].to_frame().loc[f"published_date{si}"][0],
                "trending_date":helloV1.iloc[:,i].to_frame().loc[f"trending_date{si}"][0],
                "video_title":helloV1.iloc[:,i].to_frame().loc[f"video_title{si}"][0],
                "category_title":helloV1.iloc[:,i].to_frame().loc[f"category_title{si}"][0],
                "statistics":{
                    "views": helloV1.iloc[:,i].to_frame().loc[f"views{si}"][0],
                    "likes": helloV1.iloc[:,i].to_frame().loc[f"likes{si}"][0],
                    "dislikes": helloV1.iloc[:,i].to_frame().loc[f"dislikes{si}"][0],
                    "comments": helloV1.iloc[:,i].to_frame().loc[f"comments{si}"][0],
                },
                "description": helloV1.iloc[:,i].to_frame().loc[f"description{si}"][0],
                "trending_country": helloV1.iloc[:,i].to_frame().loc[f"country{si}"][0],
                "duration": helloV1.iloc[:,i].to_frame().loc[f"duration{si}"][0],
                "link": helloV1.iloc[:,i].to_frame().loc[f"link{si}"][0],
                "thumbnail": helloV1.iloc[:,i].to_frame().loc[f"thumbnail{si}"][0],
                "trending_videos":runn_this[i]
            }
        )
    
json_object = json.dumps(empty_list_11)

loaded_r = json.loads(json_object)

with open("sample1.json","w") as outfile:
    json.dump(empty_list_11, outfile)
