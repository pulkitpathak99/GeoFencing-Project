from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class TerminalData(db.Model):
    __tablename__ = 'terminal_data'
    
    Timestamp = db.Column(db.DateTime, primary_key=True)
    SAI = db.Column(db.Integer, nullable=False)
    Device_Id = db.Column(db.String(255), nullable=False)
    SBC_Id = db.Column(db.BigInteger, nullable=False)
    Sequence_Num = db.Column(db.Integer, nullable=False)
    MLR_Option_Flag = db.Column(db.String(255), nullable=False)
    Latitude = db.Column(db.Float, nullable=False)
    Longitude = db.Column(db.Float, nullable=False)
    District = db.Column(db.String(255), nullable=False)
    State = db.Column(db.String(255), nullable=False)
    Velocity = db.Column(db.Float, nullable=False)
    Track_Angle = db.Column(db.Float, nullable=False)
    Azimuth = db.Column(db.Float, nullable=False)
    Elevation = db.Column(db.Float, nullable=False)
    Rx_Esno = db.Column(db.Float, nullable=False)
    Tx_Esno = db.Column(db.Float, nullable=False)
    RateString = db.Column(db.String(255), nullable=False)
    Modem_Output_Power = db.Column(db.Float, nullable=False)
    Cal_Ant_EIRP = db.Column(db.Float, nullable=False)
    MLR_Sat_Beam_Id = db.Column(db.String(255), nullable=False)
    MBS_Option_Flag = db.Column(db.String(255), nullable=False)
    MBS_Sat_Beam_Id = db.Column(db.String(255), nullable=False)
    Error_Index = db.Column(db.Integer, nullable=False)
    VSAT_MGMT_Addr = db.Column(db.String(255), nullable=False)
    Num_Msg_Processed = db.Column(db.Integer, nullable=False)

