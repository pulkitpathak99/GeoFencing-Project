from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit
import os
import json
import logging
from datetime import datetime, timedelta
from sqlalchemy import desc

# Initialize Flask application
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URI', 'postgresql://postgres:postgres@localhost:5432/terminal_data_db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Define models
class TerminalData(db.Model):
    __tablename__ = 'terminal_data'
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.now)
    sai = db.Column(db.String(50))
    device_id = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    district = db.Column(db.String(100))
    state = db.Column(db.String(100))

class Terminal(db.Model):
    __tablename__ = 'terminals'
    id = db.Column(db.Integer, primary_key=True)
    device_id = db.Column(db.String(50), unique=True)

# SocketIO event handlers
@socketio.on('connect')
def handle_connect():
    print('Client connected')

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

def emit_terminal_update(terminal_data):
    socketio.emit('terminal_update', terminal_data)

# Load GeoJSON data for states and districts
with open('india_districts.geojson', 'r') as f:
    data = json.load(f)
states_and_districts = {}
for feature in data['features']:
    state_name = feature['properties']['NAME_1']
    district_name = feature['properties']['NAME_2']
    states_and_districts.setdefault(state_name, []).append(district_name)

# Routes
@app.route('/')
def home():
    return render_template('home.html')

@app.route('/latest-data')
def get_latest_data():
    return render_template('index.html')

@app.route('/api/latest-terminal-data')
def get_latest_terminal_data():
    subquery = db.session.query(
        TerminalData.device_id,
        db.func.max(TerminalData.timestamp).label('max_timestamp')
    ).group_by(TerminalData.device_id).subquery()

    latest_data = db.session.query(TerminalData).join(
        subquery,
        db.and_(
            TerminalData.device_id == subquery.c.device_id,
            TerminalData.timestamp == subquery.c.max_timestamp
        )
    ).all()

    result = [{
        'timestamp': data.timestamp,
        'sai': data.sai,
        'device_id': data.device_id,
        'latitude': data.latitude,
        'longitude': data.longitude,
        'district': data.district,
        'state': data.state,
    } for data in latest_data]

    return jsonify(result)

@app.route('/api/data')
def api_data():
    latest_data = TerminalData.query.order_by(TerminalData.timestamp.desc()).limit(1).first()
    if latest_data:
        data = {key: getattr(latest_data, key) for key in latest_data.__table__.columns.keys()}
        return jsonify([data])
    return jsonify([])

@app.route('/map')
def map_page():
    return render_template('map.html')

@app.route('/path')
def path_page():
    return render_template('path.html')

@app.route('/terminal')
def terminal_page():
    return render_template('terminal.html')

import pytz

@app.route('/api/terminal-data')
def get_terminal_data():
    terminal_id = request.args.get('terminal')
    timeframe = request.args.get('timeframe', type=int)
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)

    logging.debug(f"Received request for terminal_id: {terminal_id}, timeframe: {timeframe}, page: {page}")

    if not terminal_id or not timeframe:
        logging.warning("Missing terminal_id or timeframe")
        return jsonify({'error': 'Missing terminal_id or timeframe'}), 400
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=timeframe)

    logging.debug(f"Querying data from {start_time} to {end_time}")

    try:
        query = TerminalData.query.filter(
            TerminalData.device_id == terminal_id,
            TerminalData.timestamp >= start_time
        ).order_by(desc(TerminalData.timestamp))

        # Log the SQL query
        logging.debug(f"SQL Query: {query}")

        # Get total count
        total_count = query.count()
        logging.debug(f"Total matching records: {total_count}")

        # Paginate
        pagination = query.paginate(page=page, per_page=per_page, error_out=False)
        
        result = [{
            'timestamp': d.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'latitude': d.latitude,
            'longitude': d.longitude,
            'district': d.district,
            'state': d.state
        } for d in pagination.items]

        logging.debug(f"Returning {len(result)} items, total pages: {pagination.pages}")

        return jsonify({
            'data': result,
            'total_pages': pagination.pages,
            'current_page': page,
            'total_items': total_count
        })
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/get-terminals-by-location')
def fetch_terminals_by_location():
    state = request.args.get('state', '')
    district = request.args.get('district', '')
    try:
        query = TerminalData.query
        if state:
            query = query.filter_by(state=state)
        if district:
            query = query.filter_by(district=district)
        terminals = query.order_by(TerminalData.device_id).all()
        result = [{key: getattr(t, key) for key in t.__table__.columns.keys()} for t in terminals]
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/terminals-by-location', methods=['GET'])
def get_terminals_by_location():
    state = request.args.get('state')
    district = request.args.get('district')
    
    if not state or not district:
        return jsonify({'error': 'Both state and district are required'}), 400

    try:
        terminals = TerminalData.query.filter_by(state=state, district=district).order_by(TerminalData.device_id).all()
        result = [{'device_id': terminal.device_id, 'latitude': terminal.latitude, 'longitude': terminal.longitude} for terminal in terminals]
        return jsonify(result)
    except Exception as e:
        logging.error(f"Error fetching terminals by location: {str(e)}")
        return jsonify({'error': 'Failed to fetch terminals'}), 500

@app.route('/api/path')
def get_terminal_path():
    terminal_id = request.args.get('terminal')
    timeframe = request.args.get('timeframe', type=int)
    if not terminal_id or not timeframe:
        return jsonify([])
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=timeframe)
    
    path_data = TerminalData.query.filter(
        TerminalData.device_id == terminal_id,
        TerminalData.timestamp >= start_time,
        TerminalData.timestamp <= end_time
    ).order_by(TerminalData.timestamp).all()
    
    result = [{
        'latitude': data.latitude,
        'longitude': data.longitude,
        'timestamp': data.timestamp.strftime('%Y-%m-%d %H:%M:%S')
    } for data in path_data]
    
    return jsonify(result)

@app.route('/api/states', methods=['GET'])
def get_states():
    states = list(states_and_districts.keys())
    return jsonify(states)

@app.route('/api/districts', methods=['GET'])
def get_districts():
    state = request.args.get('state')
    if state in states_and_districts:
        districts = states_and_districts[state]
        return jsonify(districts)
    else:
        return jsonify([]), 404
    
@app.route('/api/terminals', methods=['GET'])
def fetch_terminals():
    try:
        query = db.session.query(TerminalData.device_id).distinct().all()
        terminals = [{"id": t.device_id, "name": t.device_id} for t in query]
        return jsonify(terminals)
    except Exception as e:
        app.logger.error(f"Error fetching terminals: {str(e)}")
        return jsonify({'error': 'An error occurred while fetching terminals'}), 500

@app.route('/control')
def control():
    return render_template('control.html')

def get_district_polygon(state, district):
    for feature in data['features']:
        if feature['properties']['NAME_1'] == state and feature['properties']['NAME_2'] == district:
            return feature['geometry']['coordinates']
    return None

def update_terminal_status(device_id, status):
    try:
        terminal = Terminal.query.filter_by(device_id=device_id).first()
        if terminal:
            terminal.status = 'inactive' if status else 'active'
            db.session.commit()
    except Exception as e:
        app.logger.error(f"Error updating terminal status: {e}")
        db.session.rollback()

def update_all_terminals_active():
    try:
        Terminal.query.update({Terminal.status: 'active'})
        db.session.commit()
    except Exception as e:
        app.logger.error(f"Error updating terminals: {e}")
        db.session.rollback()

if __name__ == '__main__':
    socketio.run(app, debug=True)
