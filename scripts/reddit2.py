import pandas as pd
from praw import Reddit
from datetime import datetime

POST_FIELDS = ["id", "title", "score", "url", "created_utc", "num_comments", "selftext", "author"]
file_postfix = datetime.now().strftime("%Y%m%d")

def extract_posts_to_dataframe(
    client_id: str, 
    client_secret: str, 
    user_agent: str, 
    subreddit_name: str, 
    time_filter: str, 
    limit=None
) -> pd.DataFrame:
    """
    Extracts posts from Reddit and returns them as a Pandas DataFrame.
    """
    reddit = Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
    subreddit = reddit.subreddit(subreddit_name)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_list = []
    for post in posts:
        post_data = {field: getattr(post, field, None) for field in POST_FIELDS}
        post_list.append(post_data)

    df = pd.DataFrame(post_list)
    if "author" in df.columns:
        df["author"] = df["author"].apply(lambda x: str(x) if x else None)

    return df

if __name__ == "__main__":
    # Example usage
    df = extract_posts_to_dataframe(
        client_id="eNlpVJ1Pnk1fkctbdYJDAA",
        client_secret="7rO_ro2m5KmKDYuGFyKKyJmrqgaijw",
        user_agent="obicool",
        subreddit_name="python",
        time_filter="week",
        limit=50
    )
    df.to_csv(f"reddit_posts.csv_{file_postfix}", index=False)
