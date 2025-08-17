import os
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging


def fetch_trending_videos(region_code='US', max_results=10, language_code=None):
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError("YouTube API key is missing. Please set YOUTUBE_API_KEY in environment variables.")

    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        request_params = {
            'part': 'snippet,statistics,contentDetails',
            'chart': 'mostPopular',
            'regionCode': region_code,
            'maxResults': max_results
        }
        if language_code:
            request_params['hl'] = language_code

        request = youtube.videos().list(**request_params)
        response = request.execute()

        if language_code and language_code in ['ta', 'hi', 'te', 'bn', 'mr', 'gu', 'kn', 'ml', 'pa', 'ur']:
            language_specific_content = get_pure_language_content(youtube, region_code, language_code, max_results)
            if language_specific_content and len(language_specific_content) >= 5:
                response['items'] = language_specific_content

        if 'items' not in response or not response['items']:
            raise ValueError(f"No trending videos found for region {region_code}.")

        videos_data = []
        for item in response['items']:
            statistics = item.get('statistics', {}) or {}
            snippet = item.get('snippet', {}) or {}
            content_details = item.get('contentDetails', {}) or {}

            video_data = {
                'video_id': item.get('id', ''),
                'title': snippet.get('title', 'Unknown Title'),
                'channel': snippet.get('channelTitle', 'Unknown Channel'),
                'views': int(statistics.get('viewCount', 0) or 0),
                'likes': int(statistics.get('likeCount', 0) or 0),
                'comments': int(statistics.get('commentCount', 0) or 0),
                'published_at': snippet.get('publishedAt', ''),
                'description': (snippet.get('description', '')[:200] + '...') if len(
                    snippet.get('description', '')) > 200 else snippet.get('description', ''),
                'thumbnail_url': snippet.get('thumbnails', {}).get('medium', {}).get('url', ''),
                'duration': content_details.get('duration', ''),
                'channel_id': snippet.get('channelId', '')
            }
            videos_data.append(video_data)

        df = pd.DataFrame(videos_data)
        os.makedirs('data', exist_ok=True)
        lang_suffix = f"_{language_code}" if language_code else ""
        csv_path = f'data/trending_videos_{region_code}{lang_suffix}.csv'
        df.to_csv(csv_path, index=False)

        logging.info(f"Successfully fetched {len(videos_data)} videos for region {region_code}")
        return df

    except HttpError as e:
        error_details = str(e)
        if "quotaExceeded" in error_details:
            raise ValueError("YouTube API quota exceeded.")
        elif "keyInvalid" in error_details:
            raise ValueError("Invalid YouTube API key.")
        elif "forbidden" in error_details:
            raise ValueError("Access forbidden. Check API key permissions.")
        else:
            raise ValueError(f"YouTube API error: {error_details}")
    except Exception as e:
        logging.error(f"Error fetching trending videos: {str(e)}")
        raise Exception(f"Failed to fetch trending videos: {str(e)}")


def get_pure_language_content(youtube, region_code, language_code, max_results):
    try:
        from datetime import datetime, timedelta

        language_search_terms = {
            'ta': {'primary': 'tamil',
                   'queries': ['tamil movie trailer', 'tamil song', 'tamil news', 'tamil comedy', 'tamil music',
                               'tamil dance'], 'native_script': 'தமிழ்',
                   'exclude_terms': ['adult', 'explicit', '18+', 'mature']},
            'hi': {'primary': 'hindi',
                   'queries': ['hindi movie trailer', 'bollywood song', 'hindi comedy', 'hindi music', 'hindi dance',
                               'hindi news'], 'native_script': 'हिंदी',
                   'exclude_terms': ['adult', 'explicit', '18+', 'mature']},
            'te': {'primary': 'telugu',
                   'queries': ['telugu movie trailer', 'telugu song', 'tollywood', 'telugu comedy', 'telugu music',
                               'telugu dance'], 'native_script': 'తెలుగు',
                   'exclude_terms': ['adult', 'explicit', '18+', 'mature']},
            'bn': {'primary': 'bengali',
                   'queries': ['bengali movie', 'bengali song', 'kolkata', 'bengali comedy', 'bengali music',
                               'bengali dance'], 'native_script': 'বাংলা',
                   'exclude_terms': ['adult', 'explicit', '18+', 'mature']},
            'mr': {'primary': 'marathi',
                   'queries': ['marathi movie', 'marathi song', 'mumbai', 'marathi comedy', 'marathi music',
                               'marathi dance'], 'native_script': 'मराठी',
                   'exclude_terms': ['adult', 'explicit', '18+', 'mature']},
            'gu': {'primary': 'gujarati',
                   'queries': ['gujarati movie', 'gujarati song', 'gujarat', 'gujarati comedy', 'gujarati music',
                               'gujarati dance'], 'native_script': 'ગુજરાતી',
                   'exclude_terms': ['adult', 'explicit', '18+', 'mature']},
            'kn': {'primary': 'kannada',
                   'queries': ['kannada movie', 'kannada song', 'karnataka', 'kannada comedy', 'kannada music',
                               'kannada dance'], 'native_script': 'ಕನ್ನಡ',
                   'exclude_terms': ['adult', 'explicit', '18+', 'mature']},
            'ml': {'primary': 'malayalam',
                   'queries': ['malayalam movie', 'malayalam song', 'kerala', 'malayalam comedy', 'malayalam music',
                               'malayalam dance'], 'native_script': 'മലയാളം',
                   'exclude_terms': ['adult', 'explicit', '18+', 'mature']},
            'pa': {'primary': 'punjabi',
                   'queries': ['punjabi movie', 'punjabi song', 'punjab', 'punjabi comedy', 'punjabi music',
                               'punjabi dance'], 'native_script': 'ਪੰਜਾਬੀ',
                   'exclude_terms': ['adult', 'explicit', '18+', 'mature']},
            'ur': {'primary': 'urdu',
                   'queries': ['urdu movie', 'urdu song', 'urdu poetry', 'urdu comedy', 'urdu music', 'urdu dance'],
                   'native_script': 'اردو', 'exclude_terms': ['adult', 'explicit', '18+', 'mature']}
        }

        if language_code not in language_search_terms:
            return []

        lang_config = language_search_terms[language_code]
        all_videos = []

        for query in lang_config['queries'][:3]:
            try:
                search_request = youtube.search().list(
                    part='id,snippet',
                    q=query,
                    type='video',
                    regionCode=region_code,
                    maxResults=15,
                    order='relevance',
                    publishedAfter=(datetime.now() - timedelta(days=30)).isoformat() + 'Z',
                    videoDefinition='high',
                    safeSearch='strict',
                    videoDuration='medium'
                )
                search_response = search_request.execute()
                for item in search_response.get('items', []):
                    all_videos.append(item.get('id', {}).get('videoId', ''))
            except Exception as e:
                logging.warning(f"Search query '{query}' failed: {str(e)}")
                continue

        if not all_videos:
            return []

        unique_video_ids = list(dict.fromkeys(all_videos))[:max_results * 2]

        videos_request = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=','.join(unique_video_ids)
        )
        videos_response = videos_request.execute()

        filtered_videos = []
        for video in videos_response.get('items', []):
            snippet = video.get('snippet', {}) or {}
            statistics = video.get('statistics', {}) or {}

            title = snippet.get('title', '').lower()
            description = snippet.get('description', '').lower()
            channel_title = snippet.get('channelTitle', '').lower()

            exclude_terms = lang_config['exclude_terms']
            if any(term in title or term in description or term in channel_title for term in exclude_terms):
                continue

            view_count = int(statistics.get('viewCount', 0) or 0)
            if view_count < 1000:
                continue

            primary_term = lang_config['primary']
            native_script = lang_config['native_script']

            if (primary_term in title or native_script in title or
                    primary_term in description or native_script in description or
                    primary_term in channel_title):
                filtered_videos.append(video)

        filtered_videos.sort(key=lambda x: int(x.get('statistics', {}).get('viewCount', 0) or 0), reverse=True)
        return filtered_videos[:max_results]

    except Exception as e:
        logging.error(f"Error fetching pure language content: {str(e)}")
        return []
