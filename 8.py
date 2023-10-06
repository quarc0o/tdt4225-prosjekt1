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
        ORDER BY TrackPoint.lat
        LIMIT 800000;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows



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
        prev_user_id = None
        close_points = [] 

        close_users = set()

        for index, trackpoint in enumerate(some_trackpoints):
            current_id, current_lat, current_lon, current_user, current_date_days, current_date_time = trackpoint
            if current_user == prev_user_id:
                continue
            print(f"Checking User: {current_user}, Point ID: {current_id}, DateTime: {current_date_time}")
            prev_user_id = current_user
            for another_trackpoint in some_trackpoints[index:]:
                temp_id, temp_lat, temp_lon, temp_user, temp_date_days, temp_date_time = another_trackpoint
                if current_user == temp_user:
                    continue

                user_pair = frozenset([current_user, temp_user])
                if user_pair in close_users:
                    continue
                distance = program.haversine_np(current_lon, current_lat, temp_lon, temp_lat)
                if distance > 0.05:
                    break
                time_difference = abs((current_date_time - temp_date_time).total_seconds())
            
                if time_difference <= 30:
                    print("Users {} and {} are close in time with a difference of {} seconds".format(current_user, temp_user, time_difference))
                
                    # Add the trackpoints to the list
                    close_points.append((trackpoint, another_trackpoint))
                    
                    # Add the user pair to the set of close users so we skip them in subsequent comparisons
                    close_users.add(user_pair)

        print("close users: ", close_users)
        
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
