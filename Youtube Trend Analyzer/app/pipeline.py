import pandas as pd
import os
import logging
import json

def load_trending_data(region_code='US', language_code=None):
    """
    Load trending video data from CSV file.
    
    Args:
        region_code (str): Region code for the data file
        language_code (str): Language code for the data file (optional)
        
    Returns:
        pandas.DataFrame: DataFrame containing video data
        
    Raises:
        FileNotFoundError: If the data file doesn't exist
        Exception: For other data loading errors
    """
    try:
        lang_suffix = f"_{language_code}" if language_code else ""
        csv_path = f'data/trending_videos_{region_code}{lang_suffix}.csv'
        
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"No data file found for region {region_code}. Please fetch data first.")
        
        df = pd.read_csv(csv_path)
        
        if df.empty:
            raise ValueError(f"Data file for region {region_code} is empty.")
        
        logging.info(f"Successfully loaded {len(df)} records for region {region_code}")
        return df
        
    except Exception as e:
        logging.error(f"Error loading trending data: {str(e)}")
        raise Exception(f"Failed to load trending data: {str(e)}")

def get_chart_data(df):
    """
    Process DataFrame to extract data for chart visualization.
    
    Args:
        df (pandas.DataFrame): DataFrame containing video data
        
    Returns:
        tuple: (titles, views) - lists for chart rendering
    """
    try:
        if df.empty:
            return [], []
        
        # Sort by views in descending order
        df_sorted = df.sort_values('views', ascending=False)
        
        # Extract titles and views
        titles = df_sorted['title'].tolist()
        views = df_sorted['views'].tolist()
        
        # Truncate long titles for better chart display
        titles = [title[:50] + '...' if len(title) > 50 else title for title in titles]
        
        logging.info(f"Processed chart data for {len(titles)} videos")
        return titles, views
        
    except Exception as e:
        logging.error(f"Error processing chart data: {str(e)}")
        return [], []

def get_video_statistics(df):
    """
    Calculate summary statistics for the video data.
    
    Args:
        df (pandas.DataFrame): DataFrame containing video data
        
    Returns:
        dict: Dictionary containing summary statistics
    """
    try:
        if df.empty:
            return {}
        
        stats = {
            'total_videos': len(df),
            'total_views': df['views'].sum(),
            'average_views': df['views'].mean(),
            'max_views': df['views'].max(),
            'min_views': df['views'].min(),
            'total_likes': df['likes'].sum(),
            'average_likes': df['likes'].mean(),
            'total_comments': df['comments'].sum(),
            'average_comments': df['comments'].mean()
        }
        
        return stats
        
    except Exception as e:
        logging.error(f"Error calculating statistics: {str(e)}")
        return {}
