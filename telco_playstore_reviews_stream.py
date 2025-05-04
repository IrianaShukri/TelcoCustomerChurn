import time
import emoji
import pandas as pd
from google_play_scraper import reviews, Sort
from datetime import datetime
from langdetect import detect

telco_apps = {
    'Maxis': {'package': 'com.maxis.mymaxis'},
    'Digi': {'package': 'com.digi.portal.mobdev.android'},
    'Celcom': {'package': 'com.celcom.mycelcom'},
    'UMobile': {'package': 'com.omesti.myumobile'},
    'TuneTalk': {'package': 'com.tunetalk.jmango.tunetalkimsi'}
}

user_state_keywords = {
    'Kuala Lumpur': ['kl', 'kuala lumpur'],
    'Selangor': ['selangor', 'subang', 'shah alam', 'pj', 'petaling', 'gombak', 'kajang', 'sepang', 'hulu langat'],
    'Penang': ['penang', 'pulau pinang', 'georgetown', 'bukit mertajam', 'balik pulau', 'nibong tebal'],
    'Johor': ['johor', 'jb', 'johor bahru', 'batu pahat', 'muar', 'kluang', 'segamat', 'kulai', 'pontian', 'tangkak', 'mersing', 'kahang', 'labis'],
    'Sabah': ['sabah', 'kota kinabalu', 'kk', 'sandakan', 'tawau', 'keningau', 'lahad datu', 'semporna', 'beaufort', 'ranau', 'kunak', 'tenom'],
    'Sarawak': ['sarawak', 'kuching', 'miri', 'sibu', 'bintulu', 'sri aman', 'mukah', 'limbang', 'kapit', 'betong', 'sarikei'],
    'Perak': ['perak', 'ipoh', 'taiping', 'batu gajah', 'teluk intan', 'sitiawan', 'kampar', 'parit buntar', 'lenggong'],
    'Negeri Sembilan': ['seremban', 'negeri sembilan', 'port dickson', 'nilai', 'jempol', 'rembau', 'tampin', 'kuala pilah'],
    'Pahang': ['pahang', 'kuantan', 'temerloh', 'bentong', 'raub', 'maran', 'pekan', 'bera', 'jerantut', 'lipis'],
    'Malacca': ['melaka', 'malacca', 'alor gajah', 'jasin'],
    'Kelantan': ['kelantan', 'kota bharu', 'tanah merah', 'machang', 'pasir mas', 'kuala krai', 'gua musang', 'jeli', 'bachok'],
    'Terengganu': ['terengganu', 'kuala terengganu', 'dungun', 'kemaman', 'marang', 'besut', 'setiu'],
    'Kedah': ['kedah', 'alor setar', 'sungai petani', 'kulim', 'langkawi', 'pendang', 'baling', 'kuala muda'],
    'Perlis': ['perlis', 'kangar', 'arau', 'padang besar', 'kuala perlis']
}

review_categories = {
    'Login': ['login', 'sign in', 'sign up', 'register', 'authentication', 'activate', 'active', 'password', 'username'],
    'Billing': ['bill', 'reload', 'beli', 'prepaid', 'pospaid', 'ringgit', 'rm', 'kredit', 'credit', 'bil', 'bayar', 'mahal', 'murah','payment', 'invoice', 'charge', 'price', 'fee'],
    'Network': ['signal', 'network', 'line', 'coverage', 'internet', 'data', 'speed', '4g', '5g', 'wifi', 'connection'],
    'App': ['app', 'apps', 'application', 'crash', 'bug', 'update', 'install', 'version', 'load', 'freeze', 'sistem', 'system'],
    'Customer Service': ['support', 'service', 'services', 'help', 'response', 'agent', 'representative'],
    'General': ['good', 'bad', 'excellent', 'poor', 'worst', 'best', 'happy', 'angry']
}

start_date = datetime(2020, 1, 1)
end_date = datetime.now()

def detect_user_location(text):
    if not isinstance(text, str):
        return ''
    text = text.lower()
    for state, keywords in user_state_keywords.items():
        for keyword in keywords:
            if keyword in text:
                return state
    return ''

def detect_review_language(text):
    if not text or all(emoji.is_emoji(char) for char in text):
        return 'Unknown'
    try:
        lang = detect(text)
        if lang == 'id':
            return 'MS'
        elif lang == 'ms':
            return 'MS'
        elif lang == 'zh-cn':
            return 'CN'
        elif lang == 'en':
            return 'EN'
        elif lang == 'ta':
            return 'TA'
        else:
            return 'EN'
    except:
        return 'EN'

def categorize_review(text):
    if not text or pd.isna(text):
        return 'General'
    text = text.lower()
    for category, keywords in review_categories.items():
        if any(keyword in text for keyword in keywords):
            return category
    return 'General'

for telco, info in telco_apps.items():
    print(f"Streaming {telco}...")

    try:
        all_reviews = []
        next_token = None
        batch_size = 200
        max_total = 100000

        while True:
            result, next_token = reviews(
                info['package'],
                lang='en',
                country='my',
                sort=Sort.NEWEST,
                count=batch_size,
                continuation_token=next_token
            )
            
            all_reviews.extend(result)

            if not next_token:
                print("No more continuation token.")
                break

            dates = [r['at'] for r in result if 'at' in r]
            if dates and min(dates) < start_date:
                print("Reached reviews older than 2020. Stopping stream.")
                break

            if len(all_reviews) >= max_total:
                print("Reached maximum total reviews limit. Stopping stream.")
                break

            time.sleep(5)

        if not all_reviews:
            print(f"No reviews found for {telco}. Skipping.\n")
            continue

        df = pd.DataFrame(all_reviews)

        expected_cols = {'userName', 'content', 'score', 'at', 'replyContent', 'repliedAt', 'thumbsUpCount'}
        available_cols = set(df.columns)
        missing_cols = expected_cols - available_cols
        
        for col in missing_cols:
            if col == 'replyContent':
                df['replyContent'] = None
            elif col == 'repliedAt':
                df['repliedAt'] = None
            elif col == 'thumbsUpCount':
                df['thumbsUpCount'] = 0

        df = df[['userName', 'content', 'score', 'at', 'replyContent', 'repliedAt', 'thumbsUpCount']]
        df.rename(columns={
            'userName': 'Username',
            'content': 'Review',
            'score': 'Rating',
            'at': 'DateTime',
            'replyContent': 'ReviewResponse',
            'repliedAt': 'ResponseDateTime',
            'thumbsUpCount': 'ThumbsUpCount'
        }, inplace=True)

        df['DateTime'] = pd.to_datetime(df['DateTime'])
        df['ResponseDateTime'] = pd.to_datetime(df['ResponseDateTime'])
        df = df[(df['DateTime'] >= start_date) & (df['DateTime'] <= end_date)]

        if df.empty:
            print(f"No reviews from 2020 onwards for {telco}. Skipping.\n")
            continue

        df['Username'] = df['Username'].apply(
            lambda x: f"User{telco}" if x == 'A Google User' else x
        )

        df['Date'] = df['DateTime'].dt.date
        df['Time'] = df['DateTime'].dt.time
        df['Day'] = df['DateTime'].dt.day_name()
        df['ReviewLength'] = df['Review'].apply(lambda x: len(str(x).split()))
        df['ReviewLanguage'] = df['Review'].apply(detect_review_language)
        df['UserState'] = df.apply(lambda row: detect_user_location(str(row['Username']) + " " + str(row['Review'])), axis=1)
        df['UserCountry'] = 'Malaysia'
        df['ReviewCategory'] = df['Review'].apply(categorize_review)
        df['ResponseTime'] = (df['ResponseDateTime'] - df['DateTime']).dt.total_seconds() / 3600
        df['ResponseTime'] = df['ResponseTime'].apply(lambda x: '' if pd.isna(x) else x) 
        
        final_columns = [
            'Username', 'Date', 'Time', 'Day', 'Review', 'Rating', 
            'ReviewCategory', 'ReviewLength', 'ReviewLanguage', 
            'ReviewResponse', 'ResponseTime', 'ThumbsUpCount', 
            'UserState', 'UserCountry'
        ]
        
        df = df[final_columns]
        
        df.to_csv(f'{telco}_reviews.csv', index=False)
        print(f"{telco} reviews saved to {telco}_reviews.csv\n")

    except Exception as e:
        print(f"Error streaming {telco}: {e}\n")