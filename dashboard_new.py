"""
CDC Analytics Dashboard - Modular Architecture
Flask-based dashboard with Blueprint routes for CONVERSATION_SUMMARY analytics
"""

import os
import logging
from flask import Flask, render_template, request, jsonify
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load .env from the same directory as this file
ENV_PATH = Path(__file__).parent / '.env'
load_dotenv(ENV_PATH)

# Create Flask app
app = Flask(__name__)

# Import and register blueprints
from routes.analytics import analytics_bp
from routes.calls import calls_bp
from routes.churn import churn_bp
from routes.ml_quality import ml_quality_bp
from routes.new_features import new_features_bp
from routes.alerts import alerts_bp

# Register blueprints with URL prefixes
app.register_blueprint(analytics_bp, url_prefix='/api')
app.register_blueprint(calls_bp, url_prefix='/api')
app.register_blueprint(churn_bp, url_prefix='/api/churn')
app.register_blueprint(ml_quality_bp, url_prefix='/api/ml-quality')
app.register_blueprint(new_features_bp, url_prefix='/api')
app.register_blueprint(alerts_bp, url_prefix='/api/alerts')


# ==================
# Main Routes
# ==================

@app.route('/')
def dashboard():
    """Main dashboard page - supports both old and new templates"""
    # Use ?v=2 query param for new modular template
    version = request.args.get('v', '1')
    if version == '2':
        return render_template('dashboard_v2.html')
    return render_template('dashboard.html')


@app.route('/v2')
def dashboard_v2():
    """New modular dashboard with all features"""
    return render_template('dashboard_v2.html')


@app.route('/grid')
def dashboard_grid():
    """Drag-and-drop grid dashboard"""
    return render_template('dashboard_grid.html')


@app.route('/api/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'version': '2.0'})


# ==================
# Run Server
# ==================

if __name__ == '__main__':
    from routes import ORACLE_CONFIG

    print("=" * 50)
    print("CDC Analytics Dashboard (Modular)")
    print("=" * 50)
    print(f"Oracle Host: {ORACLE_CONFIG['host']}")
    print(f"Oracle Port: {ORACLE_CONFIG['port']}")
    print(f"Oracle Service: {ORACLE_CONFIG['service_name']}")
    print("=" * 50)
    print("")
    print("API Blueprints Registered:")
    print("  /api/          - Analytics (summary, sentiment, categories, etc.)")
    print("  /api/          - Calls (call details, drill-downs)")
    print("  /api/churn/    - Churn Analytics")
    print("  /api/ml-quality/ - ML Quality Management")
    print("  /api/alerts/   - Alert Configuration & History")
    print("  /api/          - New Features (heatmap, trends, products, etc.)")
    print("")
    print("=" * 50)
    print("Starting dashboard on http://localhost:5001")
    print("=" * 50)

    app.run(host='0.0.0.0', port=5001, debug=True)
