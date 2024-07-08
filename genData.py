import csv
import json
import os
import random
from psycopg2.extras import execute_values
import psycopg2
import time

from datetime import datetime
from random import uniform
from shapely.geometry import shape, Point, Polygon

#



# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': 5432,
    'user': os.getenv('DB_USER', 'postgres'),  # Replace with your PostgreSQL username
    'password': os.getenv('DB_PASSWORD', 'postgres'),  # Replace with your PostgreSQL password
    'database': os.getenv('DB_NAME', 'terminal_data_db')
}

# Load GeoJSON data for districts and states
def load_geojson(file_path):
    with open(file_path) as f:
        return json.load(f)

# Get district from coordinates
def get_district_from_coordinates(latitude, longitude, district_features):
    point = Point(longitude, latitude)
    for feature in district_features:
        district_name = feature['properties']['NAME_2']
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            return district_name
    return 'Unknown'

# Get state from coordinates
def get_state_from_coordinates(latitude, longitude, states_shapes):
    point = Point(longitude, latitude)
    for state, polygons in states_shapes.items():
        for polygon in polygons:
            if polygon.contains(point):
                return state
    return 'Unknown'

# Define India's boundary polygon
INDIA_BOUNDARY = Polygon([
    [37.109318, 75.298346], [35.860280, 79.980722], [30.453842, 81.582569], [28.879888, 80.022675],
    [26.458814, 87.989875], [27.950510, 88.124059], [27.980139, 88.845300], [26.983175, 89.013031],
    [26.953277, 91.981861], [27.817079, 91.981861], [29.378043, 96.024167], [28.246433, 97.366011],
    [27.162397, 97.097642], [21.353003, 92.615252], [23.165416, 91.393220], [24.904206, 92.454443],
    [26.171505, 89.692141], [26.563018, 88.381217], [21.853482, 89.036679], [8.224025, 77.765732],
    [23.683655, 67.931950], [27.259897, 69.597233], [35.938934, 72.497122], [37.115564, 74.621793], [37.109318, 75.298346]
])

# Generate new coordinates within India's boundary
def generate_coordinates(latitude, longitude, max_change=1):
    delta_lat = random.uniform(-max_change, max_change)
    delta_lng = random.uniform(-max_change, max_change)
    new_latitude = latitude + delta_lat
    new_longitude = longitude + delta_lng
    new_point = Point((new_longitude, new_latitude))

    if INDIA_BOUNDARY.contains(new_point):
        return round(new_latitude, 3), round(new_longitude, 3)
    else:
        if new_latitude < 21:
            new_latitude += random.uniform(0, max_change)
        elif new_longitude < 79:
            new_longitude += random.uniform(0, max_change)
        else:
            new_latitude -= random.uniform(0, max_change)
            new_longitude -= random.uniform(0, max_change)
        return round(new_latitude, 5), round(new_longitude, 5)

# Write data to CSV
def write_to_csv(data, file_path='terminal_data.csv'):
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

# Database Connection Manager
class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.connection = None

    def connect(self):
        self.connection = psycopg2.connect(**self.config)
        self.create_table_if_not_exists()  # Ensure table exists on connection

    def close(self):
        if self.connection:
            self.connection.close()

    def execute(self, query, data):
        with self.connection.cursor() as cursor:
            execute_values(cursor, query, data)

    def commit(self):
        self.connection.commit()

    def create_table_if_not_exists(self):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS terminal_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            sai INT,
            device_id TEXT,
            sbc_id INT,
            sequence_num INT,
            mlr_option_flag TEXT,
            latitude FLOAT,
            longitude FLOAT,
            district TEXT,
            state TEXT,
            velocity FLOAT,
            track_angle FLOAT,
            azimuth FLOAT,
            elevation FLOAT,
            rx_esno FLOAT,
            tx_esno FLOAT,
            rate_string TEXT,
            modem_output_power FLOAT,
            cal_ant_eirp FLOAT,
            mlr_sat_beam_id TEXT,
            mbs_option_flag TEXT,
            mbs_sat_beam_id TEXT,
            error_index INT,
            vsat_mgmt_addr TEXT,
            num_msg_processed INT,
            status VARCHAR(20) DEFAULT ACTIVE
        );
        """
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_sql)

        create_table_sql="""
        CREATE TABLE IF NOT EXISTS terminals (
        id SERIAL PRIMARY KEY,
        device_id VARCHAR(255) NOT NULL
        );
        """
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_sql)
        self.connection.commit()

# Terminal Data Generator
class TerminalDataGenerator:
    def __init__(self, db_manager, geojson_path):
        self.db_manager = db_manager
        self.device_id_start = "1712328952086-29105A"
        self.sai_start = 198086
        self.district_features = load_geojson(geojson_path)['features']
        self.states_shapes = self._load_states_shapes(self.district_features)
        self.initial_coordinates = [
            (20.5937, 78.9629), (11.059821, 78.387451), (17.12318, 79.208824),
            (29.065773, 76.040497), (27.391277, 73.432617), (15.317277, 75.713890),
            (22.309425, 72.136230), (25.096073, 85.313118), (21.251385, 81.629641),
            (26.8467088, 80.9461592)
        ]

    def _load_states_shapes(self, district_features):
        states_shapes = {}
        for feature in district_features:
            state_name = feature['properties']['NAME_1']
            polygon = shape(feature['geometry'])  # Convert geometry to Shapely polygon
            if state_name not in states_shapes:
                states_shapes[state_name] = []
            states_shapes[state_name].append(polygon)
        return states_shapes

    def generate_data(self):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data_to_write = []
        for i, coords in enumerate(self.initial_coordinates):
            latitude, longitude = generate_coordinates(coords[0], coords[1])
            district = get_district_from_coordinates(latitude, longitude, self.district_features)
            state = get_state_from_coordinates(latitude, longitude, self.states_shapes)
            device_id=self.device_id_start+ str(i)

            data = (
                timestamp, self.sai_start + i, device_id, random.randint(1000, 10000),
                random.randint(1, 1000), 'Yes' if random.random() > 0.5 else 'No', latitude, longitude,
                district, state, round(uniform(0, 120), 2), round(uniform(0, 360), 2), round(uniform(0, 360), 2),
                round(uniform(0, 90), 2), round(uniform(0, 20), 2), round(uniform(0, 20), 2),
                'Rate' + str(random.randint(1, 5)), round(uniform(0, 30), 2), round(uniform(0, 30), 2),
                'Beam_' + str(random.randint(1, 5)), 'Yes' if random.random() > 0.5 else 'No',
                'Beam_' + str(random.randint(1, 5)), random.randint(0, 10), 'Addr_' + str(random.randint(1, 100)),
                random.randint(1, 100)
            )
            data_to_write.append(data)
            self.initial_coordinates[i] = (latitude, longitude)
        return data_to_write

    def insert_data(self, data):
        insert_sql = """
        INSERT INTO terminal_data (
            timestamp, sai, device_id, sbc_id, sequence_num, mlr_option_flag,
            latitude, longitude, district, state, velocity, track_angle,
            azimuth, elevation, rx_esno, tx_esno, rate_string, modem_output_power,
            cal_ant_eirp, mlr_sat_beam_id, mbs_option_flag, mbs_sat_beam_id,
            error_index, vsat_mgmt_addr, num_msg_processed
        ) VALUES %s
        """
        self.db_manager.execute(insert_sql, data)
        insert_sql = '''
        INSERT INTO terminal (SELECT DISTINCT device_id FROM terminal_data);
        '''
        self.db_manager.execute(insert_sql)
        self.db_manager.commit()

# Main execution
def main():
    db_manager = DatabaseManager(DB_CONFIG)
    db_manager.connect()

    try:
        data_generator = TerminalDataGenerator(db_manager, 'india_districts.geojson')
        while True:
            data = data_generator.generate_data()
            data_generator.insert_data(data)
            print("Data generated and inserted at", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            time.sleep(10)
    finally:
        db_manager.close()

if __name__ == "__main__":
    main()