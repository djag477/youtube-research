import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
from math import ceil
from ops import api_key


service = build('youtube', 'v3', developerKey=api_key) # Builds the API service
chanId = "" # UPDATE with the id of the Youtube Channel. "UCgl9rHdm9KojNRWs56QI_hg" Here's Google Ads' Youtube Channel ID for testing
today = datetime.today().strftime('%Y-%m-%dT%H:%M:%SZ')
date = today


# Gets a channel id to collect the number of videos it has and returns the number of times we have to iterate to list all its videos (there's a cap of 50 videos per search request)
# Quota cost = 1
def get_pages(channel):

    request = service.channels().list(
    part='statistics', 
    id=channel
    )
    response = request.execute()
    videos = int(response['items'][0]['statistics']['videoCount']) # total number of videos
    iterations = ceil(videos/50) # there's a limit of 50 videos for each search, so we need "pages" of 50 items

    print(f"Channel has: {videos} videos")
    print(f"Which means: {iterations} pages")
    return iterations

# Gets the date to request a list of the 50 videos previous to that date and returns a dataframe with it
# Quota cost = 100
def make_report(day):

    request = service.search().list(
        part='snippet', # Contains title, description, thumbnail and publish time
        channelId=chanId,
        order="date", # Orders by date descending
        maxResults=50, # API has a limit of 50 items for request
        type='video',
        publishedBefore=day
        )

    # Produces a first data frame with the results of the first API response
    response = request.execute()
    data = pd.json_normalize(response['items'])
    df = pd.DataFrame(data)
    return df

# Returns a data frame with metrics and details for each video in the make_report() function 
# Quota cost 1 per video
def video_metrics():

    video_details_request = service.videos().list(
        part='id, snippet, statistics, topicDetails, contentDetails', 
        #forUsername=''
        id=video_ids # The previous list of video IDs comes here
        )

    video_details_response = video_details_request.execute()

    video_details_data = pd.json_normalize(video_details_response['items'])
    video_details_df = pd.DataFrame(video_details_data)
    # Creates a new column stripping the topic URL of everything but the actual topic name i.e.: [https://en.wikipedia.org/wiki/Society] --> ['Society']
    video_details_df['clean_topic_categories'] = [[ y[y.index("wiki/")+5:len(y)] for y in x] if not isinstance(x, float) else "" for x in video_details_df['topicDetails.topicCategories'] ]

    return video_details_df

# Gets the data frames from make_report() & video_metrics() functions and merges them. Also drops redundant and unnecessary columns, and returns a new data frame
def merged_data(df_a, df_b):

    # Both data frames get merged on the ID of the video
    df_cd = pd.merge(df_a, df_b, how='inner', left_on='id.videoId', right_on='id').drop(columns=[
        'etag_x','snippet.description_x','snippet.thumbnails.default.width_x','snippet.thumbnails.default.height_x',
        'snippet.thumbnails.medium.width_x','snippet.thumbnails.medium.height_x',
        'snippet.thumbnails.high.width_x','snippet.thumbnails.high.height_x','etag_y','snippet.publishedAt_y','snippet.channelId_y',
        'snippet.title_y','snippet.thumbnails.default.url_y','snippet.thumbnails.default.width_y',
        'snippet.thumbnails.default.height_y','snippet.thumbnails.medium.url_y','snippet.thumbnails.medium.width_y',
        'snippet.thumbnails.medium.height_y','snippet.thumbnails.high.url_y','snippet.thumbnails.high.width_y',
        'snippet.thumbnails.high.height_y','snippet.thumbnails.standard.url','snippet.thumbnails.standard.width',
        'snippet.thumbnails.standard.height','snippet.thumbnails.maxres.width','snippet.thumbnails.maxres.height',
        'snippet.channelTitle_y','snippet.liveBroadcastContent_y'])

    return df_cd

# Gets the id's, video tiles and tags of the merged data frame and extracts the tags, so each is assign to it's own row, out of the list they came in
# Returns a new data frame
def extract_tags(id_list, video_title, tags):
    
    id = list(id_list)
    snippet_title_y = list(video_title)
    snippet_tags = list(tags)

    kw_data= []

    for keywords in snippet_tags:
        for kwd in keywords:
            kw_data.append({'id' : id[snippet_tags.index(keywords)], 'title' : snippet_title_y[snippet_tags.index(keywords)], 'tag' : kwd})

    df_kw = pd.DataFrame(kw_data)
    return df_kw


for i in range(get_pages(chanId)):
    df = make_report(date)
    video_ids = [x for x in df["id.videoId"] if not isinstance(x, float)]
    merged_report = merged_data(df, video_metrics())
    merged_report.to_csv('report_'+str(merged_report['snippet.channelTitle_x'][0]).replace(' ','_')+f"{date}"+'.csv', index=False)
    df_kw = extract_tags(merged_report['id'], merged_report['snippet.title_x'], merged_report['snippet.tags'])
    df_kw.to_csv('tags_'+str(merged_report['snippet.channelTitle_x'][0]).replace(' ','_')+f"{date}"+'.csv')
    print("now this is the date", date)
    date = df['snippet.publishTime'].min()
