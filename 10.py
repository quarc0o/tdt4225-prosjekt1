from DbConnector import DbConnector
from tabulate import tabulate   
import numpy as np
import pandas
import time
import collections
from haversine import haversine, Unit


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def calculate_distance(self, lat1, lon1, lat2, lon2):
        coord_1 = (lat1, lon1)
        coord_2 = (lat2, lon2)
        return haversine(coord_1, coord_2, unit=Unit.METERS)  # or another unit if you prefer

    def find_longest_distance_users(self):
        # Fetch the relevant data from the database
        query = """SELECT a.user_id, a.transportation_mode, tp.lat, tp.lon, tp.date_time 
                FROM TrackPoint tp
                JOIN Activity a ON tp.activity_id = a.id
                ORDER BY a.user_id, a.transportation_mode, tp.date_time"""
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        
        user_distances = collections.defaultdict(float)
        
        # Calculate the distance and accumulate for each user/mode/date
        for index, (user_id, mode, lat, lon, dt) in enumerate(rows[:-1]):
            print(f"Processing trackpoint {index + 1} of {len(rows)}")
            _, _, next_lat, next_lon, next_dt = rows[index + 1]
            distance = self.calculate_distance(lat, lon, next_lat, next_lon)
            
            # We use date() to ignore the time part of the datetime, considering only the day
            key = (user_id, mode, dt.date())
            user_distances[key] += distance
        
        # Find the user with the longest total distance for each mode per day
        max_distances = {}  # format: {(mode): (user_id, date, distance)}
        for (user_id, mode, date), distance in user_distances.items():
            key = mode  # we just want to find max per mode
            if key not in max_distances or distance > max_distances[key][2]:
                max_distances[key] = (user_id, date, distance)
        
        # Now max_distances contains the user and day with the longest distance for each mode
        return max_distances