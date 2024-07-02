from flask import Flask, render_template, jsonify, request
import pymysql
from flask_cors import CORS
import paho.mqtt.client as mqtt
from flask_socketio import SocketIO, emit
from models import db, TerminalData, District, Terminal
import os
import json
import redis
import threading
import logging

# Initialize Tile38 client
tile38 = redis.StrictRedis(host='localhost', port=9851, decode_responses=True)

# Install pymysql as MySQLdb
pymysql.install_as_MySQLdb()

# Initialize Flask application
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Configure database URI and settings
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URI', 'mysql://root:@localhost/terminal_data_db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

# Initialize MQTT client
mqtt_client = mqtt.Client()
mqtt_client.connect("localhost", 1883, 60)
mqtt_client.loop_start()

# Connect to MySQL database using pymysql
def connect_to_mysql():
    return pymysql.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'root'),
        password=os.getenv('DB_PASSWORD', ''),
        database=os.getenv('DB_NAME', 'terminal_data_db'),
        cursorclass=pymysql.cursors.DictCursor
    )

def setup_geofences():
    with open('india_taluk.geojson', 'r') as f:
        data = json.load(f)
    for feature in data['features']:
        district_name = feature['properties']['NAME_2'].replace(' ', '_')
        state_name = feature['properties']['NAME_1'].replace(' ', '_')
        polygon = feature['geometry']
        geojson_string = json.dumps({
            "type": "Feature",
            "properties": {},
            "geometry": polygon
        })
        geofence_id = f"{state_name}_{district_name}"
        tile38.execute_command('SET', 'geofences', geofence_id, 'OBJECT', geojson_string)

socketio = SocketIO(app, cors_allowed_origins="*")

# Event handlers here
@socketio.on('connect')
def on_connect():
    emit('message', {'status': 'connected'})

@socketio.on('disconnect')
def on_disconnect():
    emit('message', {'status': 'disconnected'})

# Import additional necessary modules
from datetime import datetime

# Geofence event handling
def handle_geofence_events():
    pubsub = tile38.pubsub()
    pubsub.subscribe('geofences')
    for message in pubsub.listen():
        if message['type'] == 'message':
            data = json.loads(message['data'])
            if data['event'] == 'enter':
                on_enter_geofence(data)
            elif data['event'] == 'exit':
                on_exit_geofence(data)

geofence_thread = threading.Thread(target=handle_geofence_events)
geofence_thread.daemon = True
geofence_thread.start()

# Route for the home page
@app.route('/')
def home():
    return render_template('home.html')

# Route to get the latest data
@app.route('/latest-data')
def get_latest_data():
    connection = connect_to_mysql()
    cursor = connection.cursor()
    sql = """
    SELECT Timestamp, SAI, Device_Id, Latitude, Longitude, District, State, Status
    FROM terminal_data 
    WHERE Timestamp = (SELECT MAX(Timestamp) FROM terminal_data)
    ORDER BY SAI
    """
    cursor.execute(sql)
    result = cursor.fetchall()
    connection.close()
    return render_template('index.html', data=result)

# API route to fetch the latest data
@app.route('/api/data')
def api_data():
    connection = connect_to_mysql()
    cursor = connection.cursor()
    sql = """
    SELECT Timestamp, SAI, Device_Id, Latitude, Longitude, District, State
    FROM terminal_data 
    WHERE Timestamp = (SELECT MAX(Timestamp) FROM terminal_data)
    ORDER BY SAI
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
    connection = connect_to_mysql()
    cursor = connection.cursor()
    sql = """
    SELECT Timestamp, Latitude, Longitude, District, State
    FROM terminal_data
    WHERE Device_Id = %s AND Timestamp >= NOW() - INTERVAL %s HOUR
    ORDER BY Timestamp
    """
    cursor.execute(sql, (terminal_id, timeframe))
    data = cursor.fetchall()
    connection.close()
    return jsonify(data)

# API route to fetch path data for a specific terminal
@app.route('/api/path')
def get_terminal_path():
    terminal_id = request.args.get('terminal')
    timeframe = request.args.get('timeframe', type=int)
    if not terminal_id or not timeframe:
        return jsonify([])
    connection = connect_to_mysql()
    cursor = connection.cursor()
    sql = """
    SELECT Latitude AS latitude, Longitude AS longitude
    FROM terminal_data
    WHERE Device_Id = %s AND Timestamp >= NOW() - INTERVAL %s HOUR
    ORDER BY Timestamp
    """
    cursor.execute(sql, (terminal_id, timeframe))
    path_data = cursor.fetchall()
    connection.close()
    return jsonify(path_data)

# Load GeoJSON data for states and districts
with open('states-and-districts.geojson', 'r') as f:
    data = json.load(f)
states_and_districts = {state['state']: state['districts'] for state in data['states']}

# API route to fetch all states
@app.route('/api/states', methods=['GET'])
def get_states():
    states = list(states_and_districts.keys())
    return jsonify(states)

# API route to fetch districts for a specific state
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
    state = request.args.get('state')
    district = request.args.get('district')
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        sql = "SELECT DISTINCT Device_Id AS id, Device_Id AS name FROM terminal_data"
        if state and district:
            sql += f" WHERE State = '{state}' AND District = '{district}'"
        elif state:
            sql += f" WHERE State = '{state}'"
        cursor.execute(sql)
        terminals_data = cursor.fetchall()
        connection.close()
        return jsonify(terminals_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/terminals-by-location')
def get_terminals_by_location():
    state = request.args.get('state', '')
    district = request.args.get('district', '')
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        sql = """
        SELECT Device_Id, Latitude, Longitude, District, State
        FROM terminal_data
        WHERE (%s = '' OR State = %s) AND (%s = '' OR District = %s) AND Timestamp = (SELECT MAX(Timestamp) FROM terminal_data)
        ORDER BY Device_Id
        """
        cursor.execute(sql, (state, state, district, district))
        terminals = cursor.fetchall()
        return jsonify(terminals)
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        connection.close()

# New route to render the control page
@app.route('/control')
def control_page():
    return render_template('control.html')

@app.route('/select_district', methods=['POST'])
def select_district():
    data = request.get_json()
    state = data.get('state')
    district = data.get('district')
    connection = connect_to_mysql()
    cursor = connection.cursor()
    sql = """
    SELECT Device_Id, Latitude, Longitude, District, State
    FROM terminal_data
    WHERE State = %s AND District = %s AND Timestamp = (SELECT MAX(Timestamp) FROM terminal_data)
    ORDER BY Device_Id
    """
    cursor.execute(sql, (state, district))
    terminals = cursor.fetchall()
    connection.close()
    return jsonify(terminals)

# Modify the event handlers to control terminal transmission
def on_enter_geofence(data):
    state, district = data['state'], data['district']
    terminal_id = data['terminal_id']
    
    try:
        # Update the terminal's status in the database to 'transmission_disallowed'
        connection = connect_to_mysql()
        cursor = connection.cursor()
        sql = "UPDATE terminals SET status = 'transmission_disallowed' WHERE id = %s"
        cursor.execute(sql, (terminal_id,))
        connection.commit()
        
        # Emit an event to the frontend to notify the user
        socketio.emit('geofence_event', {'event': 'enter', 'terminal_id': terminal_id, 'state': state, 'district': district})
    except Exception as e:
        logging.error(f"Error updating terminal status: {e}")
    finally:
        connection.close()

def on_exit_geofence(data):
    state, district = data['state'], data['district']
    terminal_id = data['terminal_id']
    
    try:
        # Update the terminal's status in the database to 'transmission_allowed'
        connection = connect_to_mysql()
        cursor = connection.cursor()
        sql = "UPDATE terminals SET status = 'transmission_allowed' WHERE id = %s"
        cursor.execute(sql, (terminal_id,))
        connection.commit()
        
        # Emit an event to the frontend to notify the user
        socketio.emit('geofence_event', {'event': 'exit', 'terminal_id': terminal_id, 'state': state, 'district': district})
    except Exception as e:
        logging.error(f"Error updating terminal status: {e}")
    finally:
        connection.close()

if __name__ == '__main__':
    setup_geofences()
    socketio.run(app, debug=True)
