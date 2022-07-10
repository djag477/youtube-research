import pandas as pd
from googleapiclient.discovery import build #pip install command --> pip install google-api-python-client
from ops import api_key # Get your API key -->  https://console.cloud.google.com/apis/library/youtube.googleapis.com


# Main service to which other parameters are appendended e.g: which other specs should go on the api call
service = build('youtube', 'v3', developerKey=api_key) 

# UPDATE with the id of the Youtube Channel. "UCgl9rHdm9KojNRWs56QI_hg" Here's Google Ads' Youtube Channel ID for testing
chanId = ""


# Api call
request = service.search().list(
    part='snippet', # Contains title, description, thumbnail and publish time
    channelId=chanId,
    order="date", # Orders by date descending
    maxResults=50 # API has a limit of 50 items for request
    )


# Produces a first data frame with the results of the first API response
response = request.execute()
data = pd.json_normalize(response['items'])
df = pd.DataFrame(data)
#print(df)


# Takes the column with the ID's of the videos in order to request the metrics of these in the next call
video_ids = [x for x in df["id.videoId"] if not isinstance(x, float)]

#print(video_ids)


# Similar request to the previous one, except this time, requesting more data from each video that came in the previous results
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

# Both data frames get merged on the ID of the video
df_cd = pd.merge(df, video_details_df, how='inner', left_on='id.videoId', right_on='id').drop(columns=[
    'etag_x','snippet.description_x','snippet.thumbnails.default.width_x','snippet.thumbnails.default.height_x',
    'snippet.thumbnails.medium.width_x','snippet.thumbnails.medium.height_x',
    'snippet.thumbnails.high.width_x','snippet.thumbnails.high.height_x',
    'id.channelId','etag_y','snippet.publishedAt_y','snippet.channelId_y',
    'snippet.title_y','snippet.thumbnails.default.url_y','snippet.thumbnails.default.width_y',
    'snippet.thumbnails.default.height_y','snippet.thumbnails.medium.url_y','snippet.thumbnails.medium.width_y',
    'snippet.thumbnails.medium.height_y','snippet.thumbnails.high.url_y','snippet.thumbnails.high.width_y',
    'snippet.thumbnails.high.height_y','snippet.thumbnails.standard.url','snippet.thumbnails.standard.width',
    'snippet.thumbnails.standard.height','snippet.thumbnails.maxres.width','snippet.thumbnails.maxres.height',
    'snippet.channelTitle_y','snippet.liveBroadcastContent_y'])

#print(df_cd)


# Outputs the merged data frames with the name of the channel as name of the file
df_cd.to_csv('report_'+str(df_cd['snippet.channelTitle_x'][0]).replace(' ','_')+'.csv', index=False)

