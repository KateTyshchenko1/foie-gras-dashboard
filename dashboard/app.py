"""
Foie Gras Restaurant Analysis Dashboard
Flask application serving the visualization dashboard
"""
import json
from pathlib import Path
from flask import Flask, render_template, jsonify

app = Flask(__name__)

# Pre-compute data on startup
dashboard_data = None

def get_data():
    global dashboard_data
    if dashboard_data is None:
        data_file = Path(__file__).parent / 'data.json'
        if data_file.exists():
            with open(data_file) as f:
                dashboard_data = json.load(f)
        else:
            # Fallback to processing CSVs
            from data_processor import process_data
            dashboard_data = process_data()
    return dashboard_data


@app.route('/')
def index():
    """Serve the main dashboard page."""
    data = get_data()
    return render_template('index.html', stats=data['stats'])


@app.route('/api/stats')
def api_stats():
    """Return aggregate statistics."""
    data = get_data()
    return jsonify(data['stats'])


@app.route('/api/cities')
def api_cities():
    """Return city-level map data."""
    data = get_data()
    return jsonify(data['cities'])


@app.route('/api/states')
def api_states():
    """Return state-level aggregates."""
    data = get_data()
    return jsonify(data['states'])


@app.route('/api/cuisines')
def api_cuisines():
    """Return cuisine type distribution."""
    data = get_data()
    return jsonify(data['cuisines'])


@app.route('/api/price-bands')
def api_price_bands():
    """Return price band distribution."""
    data = get_data()
    return jsonify(data['price_bands'])


@app.route('/api/foie-sections')
def api_foie_sections():
    """Return foie gras menu section breakdown."""
    data = get_data()
    return jsonify(data.get('foie_sections', {}))


@app.route('/api/price-tier-foie')
def api_price_tier_foie():
    """Return foie gras prevalence by restaurant price tier."""
    data = get_data()
    return jsonify(data.get('price_tier_foie', {}))


@app.route('/api/foie-cuisines')
def api_foie_cuisines():
    """Return cuisine breakdown of restaurants with foie gras."""
    data = get_data()
    return jsonify(data.get('foie_cuisines', {}))


@app.route('/api/origin-data')
def api_origin_data():
    """Return origin/sourcing mentions for foie gras items."""
    data = get_data()
    return jsonify(data.get('origin_data', {}))


@app.route('/api/price-comparison')
def api_price_comparison():
    """Return price comparison data (foie gras vs all food)."""
    data = get_data()
    return jsonify(data.get('price_comparison', {}))


@app.route('/api/foie-price-dist')
def api_foie_price_dist():
    """Return foie gras price distribution."""
    data = get_data()
    return jsonify(data.get('foie_price_dist', {}))


@app.route('/api/sample-foie-items')
def api_sample_foie_items():
    """Return sample foie gras menu items with details."""
    data = get_data()
    return jsonify(data.get('sample_foie_items', []))


@app.route('/api/european-insight')
def api_european_insight():
    """Return European restaurant insight data."""
    data = get_data()
    return jsonify(data.get('european_insight', {}))


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
