import os
import logging
from flask import Flask, render_template, request, flash, redirect, url_for
from scraper import fetch_trending_videos
from pipeline import load_trending_data, get_chart_data

# Configure logging for debugging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "default_secret_key_for_development")


@app.route('/')
def home():
    """Render the home page with region selection."""
    return render_template('home.html')


@app.route('/fetch', methods=['POST'])
def fetch():
    """Fetch trending videos for the selected region and language and display dashboard."""
    try:
        region = request.form.get('region', 'US')
        language = request.form.get('language', None)

        # Map region to default language if no language specified
        region_languages = {
            # North America
            'US': 'en', 'CA': 'en', 'MX': 'es',
            # South America
            'BR': 'pt', 'AR': 'es', 'CL': 'es', 'CO': 'es', 'PE': 'es',
            # Europe
            'GB': 'en', 'DE': 'de', 'FR': 'fr', 'IT': 'it', 'ES': 'es',
            'NL': 'nl', 'BE': 'nl', 'CH': 'de', 'AT': 'de', 'SE': 'sv',
            'NO': 'no', 'DK': 'da', 'FI': 'fi', 'PL': 'pl', 'CZ': 'cs',
            'HU': 'hu', 'PT': 'pt', 'GR': 'el', 'IE': 'en', 'RU': 'ru',
            'UA': 'uk', 'TR': 'tr',
            # Asia
            'IN': 'hi', 'JP': 'ja', 'KR': 'ko', 'CN': 'zh', 'TH': 'th',
            'VN': 'vi', 'PH': 'en', 'ID': 'id', 'MY': 'ms', 'SG': 'en',
            'TW': 'zh', 'HK': 'zh', 'PK': 'ur', 'BD': 'bn', 'LK': 'si',
            'AE': 'ar', 'SA': 'ar', 'IL': 'he',
            # Africa
            'ZA': 'en', 'NG': 'en', 'KE': 'en', 'EG': 'ar', 'MA': 'ar',
            'TN': 'ar', 'GH': 'en',
            # Oceania
            'AU': 'en', 'NZ': 'en'
        }

        if not language:
            language = region_languages.get(region, 'en')

        app.logger.info(f"Fetching trending videos for region: {region}, language: {language}")

        # Fetch trending videos data
        df = fetch_trending_videos(region_code=region, max_results=10, language_code=language)

        # Process data for chart visualization
        titles, views = get_chart_data(df)

        # Convert DataFrame to list of dictionaries for template
        video_data = df.to_dict('records')

        app.logger.info(f"Successfully fetched {len(titles)} trending videos")
        return render_template('dashboard.html', titles=titles, views=views, region=region, language=language,
                               video_data=video_data)

    except ValueError as e:
        app.logger.error(f"ValueError in fetch route: {str(e)}")
        flash(f"Error: {str(e)}", 'error')
        return redirect(url_for('home'))
    except Exception as e:
        app.logger.error(f"Unexpected error in fetch route: {str(e)}")
        flash("An unexpected error occurred. Please try again.", 'error')
        return redirect(url_for('home'))


@app.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors."""
    return render_template('home.html'), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors."""
    app.logger.error(f"Internal server error: {str(error)}")
    flash("Internal server error. Please try again later.", 'error')
    return redirect(url_for('home')), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
