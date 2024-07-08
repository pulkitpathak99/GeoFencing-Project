from flask import Flask, render_template, jsonify, request
import psycopg2
from flask_cors import CORS
import paho.mqtt.client as mqtt
from flask_socketio import SocketIO, emit
from models import db, TerminalData, District, Terminal
import os
import json
import redis
import threading
import logging
from datetime import datetime

# Initialize Tile38 client
tile38 = redis.StrictRedis(host='localhost', port=9851, decode_responses=True)

# Initialize Flask application
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Configure database URI and settings
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URI', 'postgresql://user:password@localhost/terminal_data_db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

# Initialize MQTT client
mqtt_client = mqtt.Client()

# Initialize SocketIO
socketio = SocketIO(app, cors_allowed_origins="*")

# Connect to PostgreSQL database using psycopg2
def connect_to_postgresql():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'user'),
        password=os.getenv('DB_PASSWORD', 'password'),
        database=os.getenv('DB_NAME', 'terminal_data_db')
    )

# Global variable to store current geofence
current_geofence = None

# Event handlers
@socketio.on('connect')
def on_connect():
    emit('message', {'status': 'connected'})

@socketio.on('disconnect')
def on_disconnect():
    emit('message', {'status': 'disconnected'})

@socketio.on('set_geofence')
def handle_set_geofence(data):
    state = data['state']
    district = data['district']

    # Get the polygon for the district
    polygon = get_district_polygon(state, district)

    # Set the geofence in Tile38
    tile38.set(f'geofence:{state}:{district}').polygon(polygon).exec()

    # Update terminals in the database based on the new geofence
    update_terminals_geofence_status(state, district)

@socketio.on('remove_geofence')
def handle_remove_geofence(data):
    state = data['state']
    district = data['district']

    # Remove the geofence from Tile38
    tile38.del_(f'geofence:{state}:{district}').exec()

    # Update all terminals to be active
    update_all_terminals_active()

def on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode())
        device_id = payload.get('Device_Id')
        latitude = payload.get('Latitude')
        longitude = payload.get('Longitude')
        
        # Update terminal location in Tile38
        tile38.set(f'terminal:{device_id}').point(latitude, longitude).exec()
        
        # Check if the terminal is in any geofence
        geofences = tile38.intersects('geofences').get(f'terminal:{device_id}').execute()
        
        is_in_geofence = len(geofences) > 0
        update_terminal_status(device_id, is_in_geofence)
        
        # Emit a geofence update event
        socketio.emit('geofence_update', {
            'action': 'enter' if is_in_geofence else 'exit',
            'terminal': payload
        })
    
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON: {message.payload.decode()}")
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def update_terminals_geofence_status(state, district):
    # Get all terminals within the geofence
    terminals = tile38.intersects('terminals').get(f'geofence:{state}:{district}').execute()

    for terminal in terminals:
        device_id = terminal['id'].split(':')[1]
        update_terminal_status(device_id, True)

    # Emit geofence update events
    socketio.emit('bulk_geofence_update', {
        'action': 'enter',
        'terminals': [t['id'].split(':')[1] for t in terminals],
        'state': state,
        'district': district
    })

mqtt_client.on_message = on_message

def is_in_geofence(state, district):
    global current_geofence
    if current_geofence is None:
        return False
    return state == current_geofence['state'] and district == current_geofence['district']

# Route for the home page
@app.route('/')
def home():
    return render_template('home.html')

# Route to get the latest data
@app.route('/latest-data')
def get_latest_data():
    connection = connect_to_postgresql()
    cursor = connection.cursor()
    sql = """
    SELECT "Timestamp", "SAI", "Device_Id", "Latitude", "Longitude", "District", "State", "Status"
    FROM terminal_data 
    WHERE "Timestamp" = (SELECT MAX("Timestamp") FROM terminal_data)
    ORDER BY "SAI"
    """
    cursor.execute(sql)
    result = cursor.fetchall()
    connection.close()
    return render_template('index.html', data=result)

# API route to fetch the latest data
@app.route('/api/data')
def api_data():
    connection = connect_to_postgresql()
    cursor = connection.cursor()
    sql = """
    SELECT "Timestamp", "SAI", "Device_Id", "Latitude", "Longitude", "District", "State"
    FROM terminal_data 
    WHERE "Timestamp" = (SELECT MAX("Timestamp") FROM terminal_data)
    ORDER BY "SAI"
    """
    cursor.execute(sql)
    data = cursor.fetchall()
    connection.close()
    return jsonify(data)

# Route to render the map page
@app.route('/map')
def map_page():
    return render_template('map.html')

# Route to render the path page
@app.route('/path')
def path_page():
    return render_template('path.html')

# Route to render the terminal page
@app.route('/terminal')
def terminal_page():
    return render_template('terminal.html')

# API route to fetch data for a specific terminal
@app.route('/api/terminal-data')
def get_terminal_data():
    terminal_id = request.args.get('terminal')
    timeframe = request.args.get('timeframe', type=int)
    if not terminal_id or not timeframe:
        return jsonify([])
    connection = connect_to_postgresql()
    cursor = connection.cursor()
    sql = """
    SELECT "Timestamp", "Latitude", "Longitude", "District", "State"
    FROM terminal_data
    WHERE "Device_Id" = %s AND "Timestamp" >= NOW() - INTERVAL %s HOUR
    ORDER BY "Timestamp"
    """
    cursor.execute(sql, (terminal_id, timeframe))
    data = cursor.fetchall()
    connection.close()
    return jsonify(data)

@app.route('/api/get-terminals-by-location')
def fetch_terminals_by_location():
    state = request.args.get('state', '')
    district = request.args.get('district', '')
    try:
        connection = connect_to_postgresql()
        cursor = connection.cursor()
        sql = """
        SELECT "Device_Id", "Latitude", "Longitude", "District", "State"
        FROM terminal_data
        WHERE (%s = '' OR "State" = %s) AND (%s = '' OR "District" = %s) AND "Timestamp" = (SELECT MAX("Timestamp") FROM terminal_data)
        ORDER BY "Device_Id"
        """
        cursor.execute(sql, (state, state, district, district))
        terminals = cursor.fetchall()
        return jsonify(terminals)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        connection.close()

@app.route('/api/terminals-by-location', methods=['GET'])
def get_terminals_by_location():
    state = request.args.get('state')
    district = request.args.get('district')
    
    if not state or not district:
        return jsonify({'error': 'Both state and district are required'}), 400

    try:
        connection = connect_to_postgresql()
        with connection.cursor() as cursor:
            sql = """
            SELECT DISTINCT "Device_Id", "Latitude", "Longitude"
            FROM terminal_data
            WHERE "State" = %s AND "District" = %s
            ORDER BY "Device_Id"
            """
            cursor.execute(sql, (state, district))
            terminals = cursor.fetchall()

        return jsonify(terminals)
    except Exception as e:
        logging.error(f"Error fetching terminals by location: {str(e)}")
        return jsonify({'error': 'Failed to fetch terminals'}), 500
    finally:
        if connection:
            connection.close()

# API route to fetch path data for a specific terminal
@app.route('/api/path')
def get_terminal_path():
    terminal_id = request.args.get('terminal')
    timeframe = request.args.get('timeframe', type=int)
    if not terminal_id or not timeframe:
        return jsonify([])
    connection = connect_to_postgresql()
    cursor = connection.cursor()
    sql = """
    SELECT "Timestamp", "Latitude", "Longitude"
    FROM terminal_data
    WHERE "Device_Id" = %s AND "Timestamp" >= NOW() - INTERVAL %s HOUR
    ORDER BY "Timestamp"
    """
    cursor.execute(sql, (terminal_id, timeframe))
    data = cursor.fetchall()
    connection.close()
    return jsonify(data)

if __name__ == '__main__':
    socketio.run(app, debug=True)
