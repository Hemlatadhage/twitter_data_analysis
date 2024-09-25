import pandas as pd
import sqlite3

# Step 1: Load TSV file and insert data into SQLite database
def ingest_data(file_path):
    # Load the TSV file into a pandas DataFrame
    df = pd.read_csv("C:\\Users\\dell\\Downloads\\assessment25.csv")

    # Connect to SQLite (create database if it doesn't exist)
    conn = sqlite3.connect('tweets_data.db')
    cursor = conn.cursor()

    # Create table to store the tweets
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tweets (
            id TEXT PRIMARY KEY,
            author_id TEXT,
            author_handle TEXT,
            text TEXT,
            timestamp DATETIME,
            like_count INTEGER,
            retweet_count INTEGER,
            reply_count INTEGER,
            place_id TEXT
        );
    ''')

    # Insert data into the SQLite database
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT OR IGNORE INTO tweets (id, author_id, author_handle, text, timestamp, like_count, retweet_count, reply_count, place_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (row['id'], row['author_id'], row['author_handle'], row['text'], row['ts1'], row['like_count'], row['retweet_count'], row['reply_count'], row['place_id']))

    # Commit and close the database connection
    conn.commit()
    conn.close()

# Step 2: Search tweets based on a term and calculate stats
def search_tweets(term):
    conn = sqlite3.connect('tweets_data.db')
    cursor = conn.cursor()

    # 1. Tweets per day
    cursor.execute('''
        SELECT DATE(timestamp), COUNT(*) FROM tweets WHERE text LIKE ? GROUP BY DATE(timestamp)
    ''', ('%' + term + '%',))
    tweets_per_day = cursor.fetchall()

    # 2. Unique users
    cursor.execute('''
        SELECT COUNT(DISTINCT author_id) FROM tweets WHERE text LIKE ?
    ''', ('%' + term + '%',))
    unique_users = cursor.fetchone()[0]

    # 3. Average likes
    cursor.execute('''
        SELECT AVG(like_count) FROM tweets WHERE text LIKE ?
    ''', ('%' + term + '%',))
    avg_likes = cursor.fetchone()[0]

    # 4. Places (place IDs)
    cursor.execute('''
        SELECT place_id, COUNT(*) FROM tweets WHERE text LIKE ? GROUP BY place_id
    ''', ('%' + term + '%',))
    places = cursor.fetchall()

    # 5. Times of day
    cursor.execute('''
        SELECT strftime('%H', timestamp) AS hour, COUNT(*) FROM tweets WHERE text LIKE ? GROUP BY hour
    ''', ('%' + term + '%',))
    times_of_day = cursor.fetchall()

    # 6. Most active user
    cursor.execute('''
        SELECT author_handle, COUNT(*) AS tweet_count FROM tweets WHERE text LIKE ? GROUP BY author_handle ORDER BY tweet_count DESC LIMIT 1
    ''', ('%' + term + '%',))
    most_active_user = cursor.fetchone()

    conn.close()

    return {
        'tweets_per_day': tweets_per_day,
        'unique_users': unique_users,
        'avg_likes': avg_likes,
        'places': places,
        'times_of_day': times_of_day,
        'most_active_user': most_active_user
    }

# Main function to run the search in the terminal
if __name__ == '__main__':
    # Ingest data (run this once to populate the database)
    # Make sure to point to the correct TSV file path
    ingest_data("C:\\Users\\dell\\Downloads\\assessment25.csv")  # Replace with your TSV file path

    # Ask for search term
    search_term = input("Enter the term to search for: ")

    # Search and get results
    results = search_tweets(search_term)

    # Output results in a clear format
    print("\n--- Search Results for Term: '{}' ---\n".format(search_term))
    print("1. Tweets per Day:")
    for day, count in results['tweets_per_day']:
        print(f"   {day}: {count} tweets")

    print("\n2. Unique Users:", results['unique_users'])
    
    print("\n3. Average Likes:", results['avg_likes'])

    print("\n4. Places (Place IDs):")
    for place, count in results['places']:
        print(f"   {place}: {count} tweets")

    print("\n5. Times of Day (Hour):")
    for hour, count in results['times_of_day']:
        print(f"   {hour}: {count} tweets")

    if results['most_active_user']:
        print("\n6. Most Active User:", results['most_active_user'][0], "with", results['most_active_user'][1], "tweets")

