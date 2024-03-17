import googleapiclient.discovery
from pprint import pprint
import pymongo
import pandas as pd
import mysql.connector
from datetime import timedelta
import streamlit as st

api_key = "AIzaSyAPMbfnhwW-U0eS9RWKghge-WokNPwxRr4"
youtube = googleapiclient.discovery.build("youtube","v3", developerKey = api_key)

client = pymongo.MongoClient("mongodb://localhost:27017")

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Root",
    database='youtube_database_sql'
)

def get_duration_in_format(duration):
    duration = duration.replace('P', '').replace('T', '')

    hours = 0
    minutes = 0
    seconds = 0
    
    if 'H' in duration:
        hours, duration = duration.split('H')
        
    if 'M' in duration:
        minutes, duration = duration.split('M')

    if 'S' in duration:
        seconds = duration.replace('S', '')

    
    td = td = timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds))
    time = str(td)
    return time
    

def channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        maxResults = 50,
        id = channel_id  
        )
    response = request.execute()
    join_date_test = lambda ts : ts.split("T")[0]
    
    a = dict(
            title = response["items"][0]["snippet"]["title"],
            channel_id = response["items"][0]["id"],
            description = response["items"][0]["snippet"]["description"],
            join_date = join_date_test(response["items"][0]["snippet"]["publishedAt"]),
            thumbnail = response["items"][0]["snippet"]["thumbnails"]["default"]["url"],
            sub_count = response["items"][0]["statistics"]["subscriberCount"],
            video_count = response["items"][0]["statistics"]["videoCount"],
            view_count = response["items"][0]["statistics"]["viewCount"],
            upload_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            )
    return a

def get_upload_id(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        maxResults = 50,
        id = channel_id  
        )
    response = request.execute()
    upload_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    return upload_id
    

def Total_video_count(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        maxResults = 50,
        id = channel_id  
        )
    response = request.execute()
    video_count = response["items"][0]["statistics"]["videoCount"]
    return video_count
    

def get_video_id_list(up_id):
    video_id_list = []
    request = youtube.playlistItems().list(
            part="contentDetails",
            maxResults=50,
            playlistId = upload_id,
            pageToken = ""
            )
    
    response = request.execute()
    
    data = response
    per_page = response["pageInfo"]["resultsPerPage"]
    
    k = 0
        
    for i in range(0,per_page):
        x = response["items"][i]["contentDetails"]["videoId"]
        video_id_list.append(x)
        k = k + 1
        if k == int(total_video_count):
            break
      
    next_page_token = data.get('nextPageToken')
    
    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            maxResults=50,
            playlistId = upload_id,
            pageToken = next_page_token
            )
        
        response = request.execute()
        
        data = response
        per_page = response["pageInfo"]["resultsPerPage"]
           
        for i in range(0,per_page):
            x = response["items"][i]["contentDetails"]["videoId"]
            video_id_list.append(x)
            k = k + 1
            if k ==  int(total_video_count):
                break
            
        next_page_token = data.get('nextPageToken')
        if next_page_token==None:
           break
        
    return video_id_list


def video_details(vid_id_list):
    comment_count = []
    test = []
   
    for i in vid_id_list:
        request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id= i
        )
        response = request.execute()
        Video_published_date_test = lambda ts : ts.split("T")[0]
        Video_like_count_test = lambda dict,key, default=0 : dict[key] if key in dict else default
        Video_comment_count_test = lambda dict,key, default=0 : dict[key] if key in dict else default
         
        b = dict(
                 Video_id = response["items"][0]["id"],
                 Video_name = response["items"][0]["snippet"]["title"],
                 Video_description = response["items"][0]["snippet"]["description"],
                 Video_published_date = Video_published_date_test(response["items"][0]["snippet"]["publishedAt"]),
                 Video_view_count = response["items"][0]["statistics"]["viewCount"],
                 Video_like_count = Video_like_count_test( response["items"][0]["statistics"],"likeCount",0),
                 Video_favorite_count = response["items"][0]["statistics"]["favoriteCount"],
                 Video_comment_count = Video_comment_count_test( response["items"][0]["statistics"],"commentCount",0),
                 Video_duration = get_duration_in_format(response["items"][0]["contentDetails"]["duration"]),
                 Video_thumbnail = response["items"][0]["snippet"]["thumbnails"]["default"]["url"],
                 Video_caption_status = response["items"][0]["contentDetails"]["caption"],
                 Channel_id = response["items"][0]["snippet"]["channelId"]
               )
    
        comment_count.append(b["Video_comment_count"])
        
        test.append(b)

    return test


def get_comment_count(vid_id_list):
    comment_count = []

    for i in vid_id_list:
        request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id= i
        )
        response = request.execute()

        Video_comment_count_test = lambda dict,key, default=0 : dict[key] if key in dict else default

        Video_comment_count = Video_comment_count_test( response["items"][0]["statistics"],"commentCount",0)
        comment_count.append(Video_comment_count)

    return comment_count


def get_comment_details(vid_id_list):
    loop_num = 0
    k = 0
    c_details = []
    
    for i in vid_id_list:
        request = youtube.commentThreads().list(
        part="snippet,replies",
        maxResults = 10,
        videoId = i       
        )
        
        if (comment_count_list[loop_num] == 0 or comment_count_list[loop_num] == '0'):
            loop_num = loop_num +1
            continue
        response = request.execute()
        Comment_published_date_test = lambda ts : ts.split("T")[0]
    
        var = min(int(comment_count_list[loop_num])-1,10)
        loop_num = loop_num +1
        temp = 0

        for j in range(0,var):
            e = dict(
                    Comment_id = response["items"][j]["snippet"]["topLevelComment"]["id"],
                    Video_id = response["items"][j]["snippet"]["topLevelComment"]["snippet"]["videoId"],
                    Channel_id = response["items"][j]["snippet"]["topLevelComment"]["snippet"]["channelId"],
                    Comment_text = response["items"][j]["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                    Comment_author = response["items"][j]["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    Comment_published_date = Comment_published_date_test(response["items"][j]["snippet"]["topLevelComment"]["snippet"]["publishedAt"]),
                    Total_reply_count = response["items"][j]["snippet"]["totalReplyCount"]
                    )
            c_details.append(e)
            k = k + 1
            if k > var:
                break
            
            if e["Total_reply_count"] > 0:
                for temp in range (0,e["Total_reply_count"]-1):
                    g = dict(
                            Comment_id = response["items"][j]["replies"]["comments"][temp]["id"],
                            Comment_author = response["items"][j]["replies"]["comments"][temp]["snippet"]["authorDisplayName"],
                            Comment_text = response["items"][j]["replies"]["comments"][temp]["snippet"]["textDisplay"],
                            Comment_published_date = Comment_published_date_test(response["items"][j]["replies"]["comments"][temp]["snippet"]["publishedAt"]),
                            Video_id = response["items"][j]["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            Channel_id = response["items"][j]["snippet"]["topLevelComment"]["snippet"]["channelId"],
                            Total_reply_count = response["items"][j]["snippet"]["totalReplyCount"]
                        )
                    c_details.append(g)
                    k = k + 1
                    if k > var:
                        break

    return c_details


channel_id = st.text_input('Enter your channel id here')

if st.button("Scrape") and channel_id:
    channel_det = channel_details(channel_id)
    upload_id = get_upload_id(channel_id)
    total_video_count = Total_video_count(channel_id)
    vid_id_list = get_video_id_list(upload_id)
    video_det = video_details(vid_id_list)
    comment_count_list = get_comment_count(vid_id_list)
    comment_det = get_comment_details(vid_id_list)
    
    st.write(channel_det)
    st.write(video_det)
    st.write(comment_det)


if channel_id :
    channel_det = channel_details(channel_id)
    upload_id = get_upload_id(channel_id)
    total_video_count = Total_video_count(channel_id)
    vid_id_list = get_video_id_list(upload_id)    
    video_det = video_details(vid_id_list)
    comment_count_list = get_comment_count(vid_id_list)
    comment_det = get_comment_details(vid_id_list)

# Youtube API to MongoDB - channel details
if st.button("Load into MongoDB as a Datalake"):
    client
    db = client["youtube_database"]
    db

    Coll = db["channel_details"]
    Coll
    Coll.insert_one(channel_det)

    # Youtube API to MongoDB - video details
    Coll1 = db["video_details"]
    Coll1
    Coll1.insert_many(video_det)

    # Youtube API to MongoDB - comment details
    Coll2 = db["comment_details"]
    Coll2
    Coll2.insert_many(comment_det)

#MongoDB to MySQL
    
if st.button("Migrate from Datalake to SQL Tables") :
    mycursor = mydb.cursor(buffered=True)
    
    # The below set of code is commented to avoid the creation of tables that are already existing


    #mycursor
    # #mycursor.execute("CREATE DATABASE youtube_database_sql")
    # mycursor.execute("SHOW DATABASES")
    # for i in mycursor:
    #     print(i)

    # mycursor.execute("CREATE TABLE channel_details_sql (channel_id VARCHAR(255) , channel_title VARCHAR(255), channel_description TEXT, channel_published_date TIMESTAMP, channel_thumbnail VARCHAR(255) , channel_subscriber_count INT, channel_view_count INT, channel_video_count INT)")
    # mydb.commit()

    # mycursor.execute("CREATE TABLE video_details_sql ( video_id VARCHAR(255), video_name VARCHAR(255), video_description VARCHAR(2000), video_published_date TIMESTAMP, video_view_count INT, video_like_count INT, video_favorite_count INT, video_comment_count INT, video_duration TIME, video_thumbnail VARCHAR(1000), video_caption_status VARCHAR(255), channel_id VARCHAR(255))")
    # mydb.commit()

    # mycursor.execute (" CREATE TABLE comment_details_sql (comment_id VARCHAR(255), video_id VARCHAR(255), channel_id VARCHAR(255), comment_text VARCHAR(2000), comment_author VARCHAR(255), comment_published_date TIMESTAMP)") 
    # mydb.commit()

    mongo_data = Coll.find({})
    for doc in mongo_data:
        query = "INSERT INTO channel_details_sql (channel_id, channel_title, channel_description, channel_published_date, channel_thumbnail,channel_subscriber_count,channel_view_count,channel_video_count) values(%s,%s,%s,%s,%s,%s,%s,%s)"
        values = (doc["channel_id"],doc["title"],doc["description"],doc["join_date"],doc["thumbnail"],doc["sub_count"],doc["view_count"],doc["video_count"])
        
        
        mycursor.execute(query,values)

    mydb.commit()

    mongo_data = Coll1.find({})
    for doc in mongo_data:
        query = "INSERT INTO video_details_sql ( video_id , video_name , video_description , video_published_date , video_view_count , video_like_count , video_favorite_count , video_comment_count , video_duration , video_thumbnail , video_caption_status , channel_id ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        values = ( doc["Video_id"], doc["Video_name"], doc["Video_description"][0:2000], doc["Video_published_date"], doc["Video_view_count"], doc["Video_like_count"], doc["Video_favorite_count"], doc["Video_comment_count"], doc["Video_duration"], doc["Video_thumbnail"], doc["Video_caption_status"], doc["Channel_id"])
        mycursor.execute(query,values)

    mydb.commit()

    mongo_data = Coll2.find({})
    for doc in mongo_data:
        query = "INSERT INTO comment_details_sql ( comment_id, video_id, channel_id, comment_text, comment_author, comment_published_date) values(%s,%s,%s,%s,%s,%s)"
        values = ( doc["Comment_id"], doc["Video_id"], doc["Channel_id"], doc["Comment_text"][0:1500], doc["Comment_author"],doc["Comment_published_date"])
        mycursor.execute(query,values)

    mydb.commit()

# 10 sql questions

def question1():
    query1 = pd.read_sql(""" SELECT channel_details_sql.channel_id, channel_details_sql.channel_title, video_details_sql.video_name 
             FROM channel_details_sql INNER JOIN video_details_sql where channel_details_sql.channel_id = video_details_sql.channel_id
             """,mydb)
    st.table(query1)
    

def question2():
    query2 = pd.read_sql(""" SELECT channel_title, channel_video_count As Total_video_count from channel_details_sql
             WHERE channel_video_count = (SELECT MAX(channel_video_count) FROM channel_details_sql)
             ORDER BY channel_video_count DESC 
             """,mydb)
    st.table(query2)
    
    
def question3():
    query3 = pd.read_sql( """SELECT video_details_sql.video_id, video_details_sql.video_name,video_details_sql.video_view_count,
            channel_details_sql.channel_id, channel_details_sql.channel_title
            FROM video_details_sql
            INNER JOIN channel_details_sql
            WHERE video_details_sql.video_id IN (SELECT video_details_sql.video_id FROM video_details_sql ORDER BY video_details_sql.video_view_count DESC LIMIT 10 )
            """,mydb)
    st.table(query3)
    

def question4():
    query4 = pd.read_sql("""SELECT video_details_sql.video_id , video_details_sql.video_name, video_details_sql.video_comment_count
            FROM video_details_sql
            """,mydb)
    st.table(query4)
   

def question5():
    query5 = pd.read_sql(""" SELECT video_details_sql.video_id , video_details_sql.video_name, video_details_sql.video_like_count,
             channel_details_sql.channel_title
             FROM video_details_sql INNER JOIN channel_details_sql on video_details_sql.channel_id = channel_details_sql.channel_id
             WHERE video_details_sql.video_like_count = (SELECT MAX(video_details_sql.video_like_count ) FROM video_details_sql)
             """,mydb)
    st.table(query5)
  

def question6():
    query6 = pd.read_sql(""" SELECT video_details_sql.video_id , video_details_sql.video_name,video_details_sql.video_like_count 
             FROM video_details_sql
             """,mydb)
    st.table(query6)
     

def question7():
    query7 = pd.read_sql(""" SELECT channel_details_sql.channel_id, channel_details_sql.channel_title, channel_details_sql.channel_view_count
             FROM channel_details_sql
             """,mydb)
    st.table(query7)
    

def question8():
    query8 =pd.read_sql( """SELECT DISTINCT channel_details_sql.channel_title
            FROM channel_details_sql INNER JOIN video_details_sql ON video_details_sql.channel_id = channel_details_sql.channel_id
            WHERE channel_details_sql.channel_id IN
            (SELECT DISTINCT video_details_sql.channel_id
            FROM video_details_sql
            WHERE YEAR(video_details_sql.video_published_date) = 2022)
            """,mydb)
    st.table(query8)


def question9():
    query9 = pd.read_sql("""SELECT channel_details_sql.channel_title,video_details_sql.channel_id, SEC_TO_TIME(SUM(video_details_sql.video_duration))
            FROM video_details_sql INNER JOIN channel_details_sql ON video_details_sql.channel_id = channel_details_sql.channel_id
            GROUP BY video_details_sql.channel_id,channel_details_sql.channel_title
            """,mydb)
    st.table(query9)
   

def question10():
    query10 = pd.read_sql("""SELECT channel_details_sql.channel_id, channel_details_sql.channel_title 
            FROM channel_details_sql
            WHERE channel_details_sql.channel_id =
            (SELECT video_details_sql.channel_id FROM video_details_sql
            WHERE video_details_sql.video_id =
            (SELECT video_details_sql.video_id FROM video_details_sql
             WHERE video_details_sql.video_comment_count = (SELECT MAX(video_details_sql.video_comment_count) FROM video_details_sql)))
            """,mydb)
    st.table(query10)
    
  

def output_of_user_input(User_Input):
    if User_Input == 1:
        question1()
    elif User_Input == 2:
        question2()
    elif User_Input == 3:
        question3()
    elif User_Input == 4:
        question4()
    elif User_Input == 5:
        question5()
    elif User_Input == 6:
        question6()
    elif User_Input == 7:
        question7()
    elif User_Input == 8:
        question8()
    elif User_Input == 9:
        question9()
    elif User_Input == 10:
        question10()
    else:
        print("Invalid Selection")

   


def get_user_int_input():
    while True:
        try:
            user_input = input("Please enter an integer and press Enter: ")
            user_input = int(user_input)
            break
        except ValueError:
            print("That's not an integer! Please try again.")

    return user_input

st.write("""
         1. What are the names of all the videos and their correspondingChannels?
         2. Which channels have the most number of videos, and how many videos do they have?
         3. What are the Top 10 most viewed videos and their respective channels?
         4. How many comments were made on each video, what are their corresponding video names?
         5. Which videos have the highest number of likes, and what are their corresponding channel names?
         6. What is the total number of likes for each video and what are their corresponding video names?
         7  What id the total number of views for each channel and what are their corresponding channel names?
         8. What are the names of all the channels that have published videos in the year 2022
         9. What is the average duration of all the videos in each channel and what are their corresponding channel names?
         10. Which videos have the highest number of comments and what are their corresponding channel names?
         """)
User_Input = st.number_input("Enter an integer in the range of 1 - 10 to select one question from the above list ")


if User_Input:
    output_of_user_input(User_Input)
    

# mycursor.close()
# mydb.close()
