#Load libraries
import os
import requests
import json
import pandas as pd
import csv
import datetime
import time
from datetime import date
import dateutil.parser


#code to extract raw data from TwitterAPI
"""
endpoint
/2/tweets/search/recent
https://api.twitter.com/2/tweets/search/recent
"""

#Bearer_token
os.environ['TOKEN']=''

#keyword, dates, and max_results per search - as per API guidelines
#https://developer.twitter.com/en/docs/twitter-api/enterprise/rules-and-filtering/operators-by-product
keyword='raila -is:retweet'
start_date='2022-05-24T17:30:00.581Z'
end_date='2022-05-30T12:00:00.581Z'
max_results=100


def auth():
    """
    Function for authentication
    """
    return os.getenv('TOKEN')
def create_headers(bearer_token):
    """
    Function to create headers using supplied bearer token
    """
    headers={'Authorization': 'Bearer {}'.format(bearer_token)}
    return headers
def create_url(keyword,start_date,end_date,max_results):
    """
    Function to create a URL to access the endpoint with query parameters.
    """
    endpoint_url='https://api.twitter.com/2/tweets/search/recent'
    query_params={
    'query':keyword,
    'start_time':start_date,
    'end_time':end_date,
    'max_results':max_results,
    'expansions':'attachments.poll_ids,attachments.media_keys,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id',
    'tweet.fields':'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld',
    'user.fields':'created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld',
    'next_token':{}
    }
    return(endpoint_url,query_params)
def connect_to_endpoint(url,headers,params,next_token=None):
    """
    Function to connect to the created URL and return response in JSON format
    """
    params['next_token']=next_token
    r=requests.get(url,headers=headers,params=params)
    #print('Response: ' +str(r.status_code))
    if r.status_code!=200:
        raise Exception(r.status_code,r.text)
    return r.json()

#Call functions
bearer_token=auth()
headers=create_headers(bearer_token)
url=create_url(keyword,start_date,end_date,max_results)
json_response=connect_to_endpoint(url[0],headers,url[1])

today=date.today()

#Create a json file to dump the JSON responses
with open('tweeters'+keyword+str(today)+'.json','w') as f:
    json.dump(json_response,f)

#Initialize lists to save next tokens (pagination) and tweets obtained
next_tokens=[]
tweets=[]

#While loop to retrieve and append next tokens to the initialized list
while 'next_token' in json_response['meta']:
    next_token=json_response['meta']['next_token']
    # next_tokens_lst=next_token.split()
    # print(type(next_tokens_lst))
    next_tokens.append(next_token)
    bearer_token=auth()
    headers=create_headers(bearer_token)
    json_response=connect_to_endpoint(url[0],headers,url[1],next_token)

#For-loop to access each next token in the next tokens list
for next_token in next_tokens:
    #Call functions
    bearer_token=auth()
    headers=create_headers(bearer_token)
    json_response=connect_to_endpoint(url[0],headers,url[1],next_token)
    #Append JSON response to tweets list
    tweets.append(json_response)
# print(tweets)
    def append_to_csv(json_response,fileName):
        """
        Function to append JSON response to CSV file
        """
        #Count tweets retrieved
        counter=0
        #Open and write CSV file
        csv_file=open(fileName,'a',newline='',encoding='utf-8')
        csv_writer=csv.writer(csv_file)
        #Rows to include
        csv_writer.writerow(['created_at','id','location','url','username','followers_count','following_count','retweet_count','reply_count','like_count','quote_count','source','text'])

        #Loop through all tweets
        for tweet in json_response['data']:
            #Retrieve tweet ID
            id=tweet['id']
            #Retrieve when tweet was created
            created_at=dateutil.parser.parse(tweet['created_at'])
            # description=tweet['context_annotations']['domain']['description']
            #Retrieve location if not blank
            if ('location' in tweet):
                location=tweet['location']
            else:
                location=''
            #Retrieve URL if not blank
            if ('url' in tweet):
                url=tweet['urls']['url']
            else:
                url=''
            #Retrieve tweep's username if not blank
            if ('username' in tweet):
                username=tweet['username']
            else:
                username=''
            #Retrieve tweep's followers count if not blank
            if 'followers_count' in tweet:
                followers_count=tweet['public_metrics']['followers_count']
            else:
                followers_count=''
            #Retrieve tweep's following count if not blank
            if 'following_count' in tweet:
                following_count=tweet['public_metrics']['following_count']
            else:
                following_count=''
            #Retrieve retweet count
            retweet_count=tweet['public_metrics']['retweet_count']
            #Retrieve reply count
            reply_count=tweet['public_metrics']['reply_count']
            #Retrieve like count
            like_count=tweet['public_metrics']['like_count']
            #Retrieve quoted count
            quote_count=tweet['public_metrics']['quote_count']
            #Retrieve tweet's source
            source=tweet['source']
            #Retrieve tweet's text
            text=tweet['text']

            #List containing retrieved fields
            response=[created_at,id,location,url,username,followers_count,following_count,retweet_count,reply_count,like_count,quote_count,source,text]
            #Write a CSV row with retrieved fields
            csv_writer.writerow(response)
            #Add to counter
            counter+=1
        #Close file
        csv_file.close()
        # print('Retrieving tweets...')
        print('Number of tweets added from response are:',counter)
        #Sleep to avoid exceeding API's quota
        time.sleep(15)
    #Call function to save JSON response to CSV
    append_to_csv(json_response,'tweeters'+keyword+str(today)+'.csv')

print('Done!')
