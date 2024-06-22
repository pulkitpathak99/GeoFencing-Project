from flask import Flask, render_template, jsonify, request
import pymysql
import os
import json
from flask_cors import CORS
from models import db, TerminalData

pymysql.install_as_MySQLdb()

app = Flask(__name__)
CORS(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:@localhost/terminal_data_db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

def connect_to_mysql():
    connection = pymysql.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'root'),
        password=os.getenv('DB_PASSWORD', ''),
        database=os.getenv('DB_NAME', 'terminal_data_db'),
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/latest-data')
def get_latest_data():
    connection = connect_to_mysql()
    cursor = connection.cursor()

    sql = """
    SELECT Timestamp, SAI, Device_Id, Latitude, Longitude, District, State
    FROM terminal_data 
    WHERE Timestamp = (SELECT MAX(Timestamp) FROM terminal_data)
    ORDER BY SAI
    """
    cursor.execute(sql)
    result = cursor.fetchall()
    connection.close()

    return render_template('index.html', data=result)

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

@app.route('/map')
def map_page():
    return render_template('map.html')

@app.route('/path')
def path_page():
    return render_template('path.html')

@app.route('/terminal')
def terminal_page():
    return render_template('terminal.html')

@app.route('/api/terminals')
def fetch_terminals():
    try:
        # Using distinct to ensure unique Device_Id
        terminals = TerminalData.query.with_entities(TerminalData.Device_Id).distinct().all()

        terminals_data = [{
            'id': terminal.Device_Id,
            'name': terminal.Device_Id
        } for terminal in terminals]

        return jsonify(terminals_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


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

with open('states-and-districts.geojson', 'r') as f:
    data = json.load(f)

states_and_districts = {state['state']: state['districts'] for state in data['states']}

@app.route('/api/districts', methods=['GET'])
def get_districts():
    state = request.args.get('state')
    if state in states_and_districts:
        return jsonify(states_and_districts[state])
    else:
        return jsonify([]), 404  # Return an empty list if state is not found

@app.route('/api/states', methods=['GET'])
def get_states():
    return jsonify(list(states_and_districts.keys()))

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)

