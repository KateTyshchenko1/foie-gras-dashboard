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


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
