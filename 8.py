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
        SELECT TrackPoint.id, TrackPoint.lat, TrackPoint.lon, Activity.user_id
        FROM TrackPoint
        JOIN Activity ON TrackPoint.activity_id = Activity.id
        LIMIT 500
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        lon = [row[2] for row in rows]
        lat = [row[1] for row in rows]
        user_ids = [row[3] for row in rows]
        return lon, lat, user_ids



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
        lon = some_trackpoints[0]
        lat = some_trackpoints[1]
        user_id = some_trackpoints[2]
        count = 0
        for some_lon, some_lat, some_user in zip(lon, lat, user_id):
            count += 1
            #print("comparing: ", some_lon, some_lat)
            df = pandas.DataFrame(data={'lon1':lon,'lon2':some_lon,'lat1':lat,'lat2':some_lat})
            km = program.haversine_np(df['lon1'],df['lat1'],df['lon2'],df['lat2'])
        print("Finished comparing ", count, " trackpoints")
        
    
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