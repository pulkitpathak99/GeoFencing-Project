import csv
import json
import os
import random
import time
from datetime import datetime
from random import uniform
from shapely.geometry import shape, Point, Polygon
import pymysql

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
india_boundary = Polygon([
    [37.109318, 75.298346], [35.860280, 79.980722], [30.453842, 81.582569], [28.879888, 80.022675],
    [26.458814, 87.989875], [27.950510, 88.124059], [27.980139, 88.845300], [26.983175, 89.013031],
    [26.953277, 91.981861], [27.817079, 91.981861], [29.378043, 96.024167], [28.246433, 97.366011],
    [27.162397, 97.097642], [21.353003, 92.615252], [23.165416, 91.393220], [24.904206, 92.454443],
    [26.171505, 89.692141], [26.563018, 88.381217], [21.853482, 89.036679], [8.224025, 77.765732],
    [23.683655, 67.931950], [27.259897, 69.597233], [35.938934, 72.497122], [37.115564, 74.621793], [37.109318, 75.298346]
])

# Generate new coordinates within India's boundary
def generate_coordinates(latitude, longitude, max_change=0.05):
    delta_lat = random.uniform(-max_change, max_change)
    delta_lng = random.uniform(-max_change, max_change)
    new_latitude = latitude + delta_lat
    new_longitude = longitude + delta_lng
    new_point = Point((new_longitude, new_latitude))
    
    if india_boundary.contains(new_point):
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
def write_to_csv(data):
    with open('terminal_data.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

# Generate and insert data into MySQL
# Generate and insert data into MySQL
def generate_and_insert_data():
    connection = pymysql.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'root'),
        password=os.getenv('DB_PASSWORD', ''),
        database=os.getenv('DB_NAME', 'terminal_data_db')
    )
    try:
        cursor = connection.cursor()

        device_id_start = "1712328952086-29105A"
        sai_start = 198086

        district_features = load_geojson('india_taluk.geojson')['features']
        states_shapes = {}
        for feature in district_features:
            state_name = feature['properties']['NAME_1']
            polygon = shape(feature['geometry'])  # Convert geometry to Shapely polygon
            if state_name not in states_shapes:
                states_shapes[state_name] = []
            states_shapes[state_name].append(polygon)

        initial_coordinates = [
            (20.5937, 78.9629), (11.059821, 78.387451), (17.12318, 79.208824),
            (29.065773, 76.040497), (27.391277, 73.432617), (15.317277, 75.713890),
            (22.309425, 72.136230), (25.096073, 85.313118), (21.251385, 81.629641),
            (26.8467088, 80.9461592)
        ]

        while True:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data_to_write = []
            for i, coords in enumerate(initial_coordinates):
                latitude, longitude = generate_coordinates(coords[0], coords[1])
                district = get_district_from_coordinates(latitude, longitude, district_features)
                state = get_state_from_coordinates(latitude, longitude, states_shapes)

                data = (
                    timestamp, sai_start + i, device_id_start + str(i), random.randint(1000, 10000),
                    random.randint(1, 1000), 'Yes' if random.random() > 0.5 else 'No', latitude, longitude,
                    district, state, round(uniform(0, 120), 2), round(uniform(0, 360), 2), round(uniform(0, 360), 2),
                    round(uniform(0, 90), 2), round(uniform(0, 20), 2), round(uniform(0, 20), 2),
                    'Rate' + str(random.randint(1, 5)), round(uniform(0, 30), 2), round(uniform(0, 30), 2),
                    'Beam_' + str(random.randint(1, 5)), 'Yes' if random.random() > 0.5 else 'No',
                    'Beam_' + str(random.randint(1, 5)), random.randint(0, 10), 'Addr_' + str(random.randint(1, 100)),
                    random.randint(1, 100)
                )
                data_to_write.append(data)
                insert_sql = """
                INSERT INTO terminal_data (
                    Timestamp, SAI, Device_Id, SBC_Id, Sequence_Num, MLR_Option_Flag,
                    Latitude, Longitude, District, State, Velocity, Track_Angle,
                    Azimuth, Elevation, Rx_Esno, Tx_Esno, RateString, Modem_Output_Power,
                    Cal_Ant_EIRP, MLR_Sat_Beam_Id, MBS_Option_Flag, MBS_Sat_Beam_Id,
                    Error_Index, VSAT_MGMT_Addr, Num_Msg_Processed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, data)
                initial_coordinates[i] = (latitude, longitude)

            write_to_csv(data_to_write)
            connection.commit()
            print("Done for", timestamp)
            time.sleep(10)
    finally:
        connection.close()


# Main execution
if __name__ == "__main__":
    generate_and_insert_data()

