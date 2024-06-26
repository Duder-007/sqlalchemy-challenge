from flask import Flask, jsonify
import pandas as pd
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

app = Flask(__name__)

# Database setup
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)

# References to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

# Route: Homepage
@app.route('/')
def welcome():
    return (
        f"Welcome to the Climate API!<br/>"
        f"Available Routes:<br/>"
        f"<a href='/api/v1.0/precipitation'>/api/v1.0/precipitation</a><br/>"
        f"<a href='/api/v1.0/stations'>/api/v1.0/stations</a><br/>"
        f"<a href='/api/v1.0/tobs'>/api/v1.0/tobs</a><br/>"
        f"<a href='/api/v1.0/<start>'>/api/v1.0/&lt;start&gt;</a><br/>"
        f"<a href='/api/v1.0/<start>/<end>'>/api/v1.0/&lt;start&gt;/&lt;end&gt;</a><br/>"
    )

# Route: Precipitation
@app.route('/api/v1.0/precipitation')
def precipitation():
    # Create session from Python to the DB
    session = Session(engine)

    # Calculate the date one year from the last date in data set
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    cutoff_date = (pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')

    # Query for the last 12 months of precipitation data
    prcp_results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= cutoff_date).all()

    # Close session
    session.close()

    # Convert the query results to a dictionary
    prcp_dict = {date: prcp for date, prcp in prcp_results}

    return jsonify(prcp_dict)

# Route: Stations
@app.route('/api/v1.0/stations')
def stations():
    # Create session from Python to the DB
    session = Session(engine)

    # Query all stations
    stations_results = session.query(Station.station, Station.name).all()

    # Close session
    session.close()

    # Convert the query results to a list of dictionaries
    stations_list = [{"station": station, "name": name} for station, name in stations_results]

    return jsonify(stations_list)

# Route: Temperature Observations (tobs)
@app.route('/api/v1.0/tobs')
def tobs():
    # Create session from Python to the DB
    session = Session(engine)

    # Calculate the date one year from the last date in data set
    most_recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    cutoff_date = (pd.to_datetime(most_recent_date) - pd.DateOffset(years=1)).strftime('%Y-%m-%d')

    # Find the most active station (the station with the highest number of observations)
    most_active_station = session.query(Measurement.station).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()[0]

    # Query the dates and temperature observations of the most-active station for the previous year of data
    tobs_results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == most_active_station).filter(Measurement.date >= cutoff_date).all()

    # Close session
    session.close()

    # Convert the query results to a list of dictionaries
    tobs_list = [{"date": date, "tobs": tobs} for date, tobs in tobs_results]

    return jsonify(tobs_list)

# Route: Temperature Stats for Start Date
@app.route('/api/v1.0/<start>')
def temp_stats_start(start):
    # Create session from Python to the DB
    session = Session(engine)

    # Query min, avg, max temperatures for all dates greater than or equal to the start date
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start).all()

    # Close session
    session.close()

    # Convert the query results to a dictionary
    temp_stats = {
        "start_date": start,
        "TMIN": results[0][0],
        "TAVG": results[0][1],
        "TMAX": results[0][2]
    }

    return jsonify(temp_stats)

# Route: Temperature Stats for Start-End Date Range
@app.route('/api/v1.0/<start>/<end>')
def temp_stats_start_end(start, end):
    # Create session from Python to the DB
    session = Session(engine)

    # Query min, avg, max temperatures for the dates from start to end (inclusive)
    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start).filter(Measurement.date <= end).all()

    # Close session
    session.close()

    # Convert the query results to a dictionary
    temp_stats = {
        "start_date": start,
        "end_date": end,
        "TMIN": results[0][0],
        "TAVG": results[0][1],
        "TMAX": results[0][2]
    }

    return jsonify(temp_stats)

if __name__ == '__main__':
    app.run(debug=True)
