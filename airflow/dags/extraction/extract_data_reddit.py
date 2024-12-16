import configparser
import datetime
import pandas as pd
import praw
import pathlib
import sys
import numpy as np
from val_date import validate_input


# just for display max col width
# pd.set_option('display.max_colwidth', None)

parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
conf_file = f"{script_path}/config.conf"
parser.read(conf_file)
SECRET = parser.get("reddit_config", "secret")
CLIENT_ID = parser.get("reddit_config", "client_id")

SUBREDDIT = "DotA2"
TIME_FILTER = "day"
LIMIT = None
POST_FIELDS = (
    "id",
    "title",
    "score",
    "num_comments",
    "author",
    "created_utc",
    "url",
    "upvote_ratio",
    "over_18",
    "edited",
    "spoiler",
    "stickied",
)

try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Error with file input. Error {e}")
    sys.exit(1)
    
date_dag_run = datetime.datetime.strptime(output_name, "%Y%m%d")
    
def main():
    validate_input(output_name)
    reddit_instance = api_connect()
    subreddit_post_object = subreddit_post(reddit_instance)
    extracted_post_data = extract_post_data(subreddit_post_object)
    transformed_data = transform_data(extracted_post_data)
    save_to_csv(transformed_data)
    
 
def api_connect():
    """
    Connect to Reddit API
    """
    try:
        instance = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=SECRET,
            user_agent="my user agent",
        )
        return instance
        
    except Exception as e:
        print(f"Unable to connect to API. Error: {e}")
        sys.exit(1)
        
def subreddit_post(reddit_instance):
    try:
        subreddit = reddit_instance.subreddit(SUBREDDIT)
        posts = subreddit.top(time_filter=TIME_FILTER, limit=LIMIT)
        return posts
    except Exception as e:
        print(f"Eror: {e}")
        sys.exit(1)

def extract_post_data(posts):
    list_of_items = []
    try:
        for submission in posts:
            to_dict = vars(submission)
            sub_dict = {field: to_dict[field] for field in POST_FIELDS}
            sub_dict['url'] = f"https://www.reddit.com/{submission.permalink}"
            list_of_items.append(sub_dict)
            extract_post_data_df = pd.DataFrame(list_of_items)
        return extract_post_data_df
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def transform_data(df):
    
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')
    df['over_18'] = np.where((df['over_18'] == 'False') | (df['over_18'] == False), False, True).astype(bool)
    df['edited'] = np.where((df['edited'] == 'False') | (df['edited'] == False), False, True).astype(bool)
    df['spoiler'] = np.where((df['spoiler'] == 'False') | (df['spoiler'] == False), False, True).astype(bool)
    df['stickied'] = np.where((df['stickied'] == 'False') | (df['stickied'] == False), False, True).astype(bool)
    
    return df

def save_to_csv(final_df):
    output_file_path = f"/tmp/{output_name}.csv"
    final_df.to_csv(output_file_path, index=False)
    print(f"File saved successfully: {output_file_path}")
    

if __name__ == "__main__":
    main()
    
# subred = subreddit_post(api_connect())
# ext = extract_post_data(subred)
# final_df = transform_data(ext)

# print(ext.info())
# print(final_df.info())

# print(ext['created_utc'].head())
# print(ext.info())