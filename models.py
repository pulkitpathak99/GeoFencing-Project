from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.mysql import VARCHAR

# Initialize the database
db = SQLAlchemy()

class TerminalData(db.Model):
    __tablename__ = 'terminal_data'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    timestamp = db.Column(db.DateTime, nullable=False)
    sai = db.Column(db.String(255), nullable=False)
    device_id = db.Column(db.String(255), nullable=False)
    latitude = db.Column(db.Float, nullable=False)
    longitude = db.Column(db.Float, nullable=False)
    district = db.Column(db.String(255), nullable=False)
    state = db.Column(db.String(255), nullable=False)
    
    def __repr__(self):
        return f"<TerminalData {self.device_id}>"

class District(db.Model):
    __tablename__ = 'districts'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    state = db.Column(db.String(255), nullable=False)
    district = db.Column(db.String(255), nullable=False)
    geometry = db.Column(db.Text, nullable=False)  # GeoJSON data stored as text
    
    def __repr__(self):
        return f"<District {self.district}, {self.state}>"

class Terminal(db.Model):
    __tablename__ = 'terminals'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    device_id = db.Column(db.String(255), nullable=False, unique=True)
    name = db.Column(db.String(255), nullable=False)
    last_latitude = db.Column(db.Float, nullable=True)
    last_longitude = db.Column(db.Float, nullable=True)
    last_timestamp = db.Column(db.DateTime, nullable=True)
    
    def __repr__(self):
        return f"<Terminal {self.device_id}>"

# Utility function to create all tables
def create_tables():
    db.create_all()
