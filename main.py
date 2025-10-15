import os
import requests

CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')
REDIRECT_URI = 'http://localhost:8888'

def get_oauth_token(client_id, client_secret):
    url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    return response.json()['access_token']

def get_top_russian_streamers(token, client_id, top_n=20):
    url = 'https://api.twitch.tv/helix/streams'
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {token}'
    }
    params = {
        'language': 'ru',
        'first': top_n
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()['data']

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("Error: env variables is not set: TWITCH_CLIENT_ID,TWITCH_CLIENT_SECRET")
        return
    token = get_oauth_token(CLIENT_ID, CLIENT_SECRET)
    streams = get_top_russian_streamers(token, CLIENT_ID)
    print(f"Top {len(streams)} Russian streamers om Twitch:")
    for stream in streams:
        print(f"{stream['user_name']} - {stream['title']} - {stream['viewer_count']} current views")

if __name__ == '__main__':
    main()
