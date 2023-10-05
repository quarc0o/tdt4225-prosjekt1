from DbConnector import DbConnector
from tabulate import tabulate   
import numpy as np
import pandas
import time

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def get_users(self):
        self.cursor.execute("SELECT * FROM User")
        rows = self.cursor.fetchall()
        return rows
    
    def get_activity(self):
        self.cursor.execute("SELECT * FROM Activity")
        rows = self.cursor.fetchall()
        return rows
    
    def get_trackpoint(self):
        query = """
        SELECT TrackPoint.id, TrackPoint.lat, TrackPoint.lon, Activity.user_id, TrackPoint.date_days, TrackPoint.date_time
        FROM TrackPoint
        JOIN Activity ON TrackPoint.activity_id = Activity.id
        LIMIT 30000
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        lon = [row[2] for row in rows]
        lat = [row[1] for row in rows]
        user_ids = [row[3] for row in rows]
        date_days = [row[4] for row in rows]
        date_time = [row[5] for row in rows] 
        return lon, lat, user_ids, date_days, date_time



    def haversine_np(self, lon1, lat1, lon2, lat2):
        lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
        
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        
        a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
        
        c = 2 * np.arcsin(np.sqrt(a))
        km = 6378.137 * c
        return km
    
    def get_user_by_trackpoint(self, trackpoint_id):
        try:
            query = """
            SELECT User.*
            FROM User, Activity, TrackPoint
            WHERE TrackPoint.id = %s
                AND TrackPoint.activity_id = Activity.id
                AND Activity.user_id = User.id
            """
            self.cursor.execute(query, (trackpoint_id, ))
            user = self.cursor.fetchone()
            return user
        except Exception as e:
            print(f"ERROR: Unable to fetch user for trackpoint {trackpoint_id}: {e}")
            return None


def main():
    start_time = time.time()
    program = None
    try:
        program = ExampleProgram()
        some_trackpoints = program.get_trackpoint()
        lon, lat, user_id, date_days, date_time = some_trackpoints
        count = 0
        track_count = 0
        
        for i, (lon1, lat1, user1, time1) in enumerate(zip(lon, lat, user_id, date_time)):
            track_count += 1
            for j, (lon2, lat2, user2, time2) in enumerate(zip(lon, lat, user_id, date_time)):
                # Check if the trackpoints are not from the same user
                if user1 != user2:
                    # Check if timestamps are within 30 minutes of each other
                    time_diff = abs((time1 - time2).total_seconds())
                    if time_diff <= 1800:  # 30 minutes = 1800 seconds
                        # Calculate the distance between the two points
                        km = program.haversine_np(lon1, lat1, lon2, lat2)
                        # Check if the distance is less than 0.1 km
                        if km < 0.1:
                            count += 1
                            print(f"Trackpoint {i} ({lon1}, {lat1}, User: {user1}, Time: {time1}) is close to "
                                  f"Trackpoint {j} ({lon2}, {lat2}, User: {user2}, Time: {time2}) with distance {km:.5f} km")
        
        print("Finished comparing", track_count, "trackpoints. Found", count, "trackpoint pairs closer than 0.1 km, within 30 minutes, and not from the same user.")
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"The program took {elapsed_time:.2f} seconds to run.")


if __name__ == '__main__':
    main()
