from DbConnector import DbConnector
from tabulate import tabulate
import os
import datetime    

def standardize_date_format(date_str):
        try:
            # Try converting from YYYY/MM/DD format
            return datetime.datetime.strptime(date_str, '%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            # Else, assume it's already in YYYY-MM-DD format
            return date_str

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        userTable = """CREATE TABLE IF NOT EXISTS User (
                    id VARCHAR(5) NOT NULL PRIMARY KEY,
                    has_labels BOOLEAN NOT NULL)
                    """
        activityTable = """CREATE TABLE IF NOT EXISTS Activity (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    user_id VARCHAR(5) NOT NULL,
                    transportation_mode VARCHAR(30),
                    start_date_time DATETIME NOT NULL,
                    end_date_time DATETIME NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES User(id))
                    """
        trackpointTable = """CREATE TABLE IF NOT EXISTS TrackPoint (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    activity_id INT NOT NULL,
                    lat DOUBLE NOT NULL,
                    lon DOUBLE NOT NULL,
                    altitude INT NOT NULL,
                    date_days DOUBLE NOT NULL,
                    date_time DATETIME NOT NULL,
                    FOREIGN KEY(activity_id) REFERENCES Activity(id))
                    """
        self.cursor.execute(userTable)
        self.cursor.execute(activityTable)
        self.cursor.execute(trackpointTable)
        self.db_connection.commit()

    

    def read_labels(self, user_path):
        labels_path = os.path.join(user_path, "labels.txt")
        if not os.path.exists(labels_path):
            return []
        with open(labels_path, 'r') as my_file:
            lines = my_file.readlines()[1:]  
        activities = [line.strip().split('\t') for line in lines]
        return activities
    
    def read_trackpoints(self, trackpoint_path):
        with open(trackpoint_path, 'r') as my_file:
            lines = my_file.readlines()[6:]  
        if len(lines) > 2500: 
            return None
        trackpoints = [line.strip().split(',') for line in lines]
        return trackpoints

    def read_trajectory_files(self, trajectory_path):
        files = [os.path.join(trajectory_path, f) for f in os.listdir(trajectory_path) if f.endswith('.plt')]
        print("file", files)
        all_points = []
        for some_file in files:
            full_path = os.path.join(trajectory_path, some_file)
            print("Full path to plt file:", full_path)
            with open(some_file, 'r') as f:
                points = f.readlines()[6:]  
                all_points.extend([point.strip().split(',') for point in points])
        return all_points

    def insert_user(self, user_id, has_labels):
        query = "INSERT INTO User (id, has_labels) VALUES (%s, %s)"
        self.cursor.execute(query, (user_id, has_labels))
        self.db_connection.commit()

    def insert_activity(self, user_id, mode, start_time, end_time):
        query = """INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) 
                VALUES (%s, %s, %s, %s)"""
        self.cursor.execute(query, (user_id, mode, start_time, end_time))
        self.db_connection.commit()
        return self.cursor.lastrowid  

    def insert_trackpoints(self, activity_id, trackpoints):
        query = """INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) 
                    VALUES (%s, %s, %s, %s, %s, %s)"""
        self.cursor.executemany(query, trackpoints)
        self.db_connection.commit()

    def insert_trackpoints_batch(self, trackpoint_data):
        query = """INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) 
                    VALUES (%s, %s, %s, %s, %s, %s)"""
        self.cursor.executemany(query, trackpoint_data)
        self.db_connection.commit()



    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE IF EXISTS %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def get_mode_from_labels(self, labels, start_time, end_time):
        start_time_dt = datetime.datetime.strptime(standardize_date_format(start_time), '%Y-%m-%d %H:%M:%S')
        end_time_dt = datetime.datetime.strptime(standardize_date_format(end_time), '%Y-%m-%d %H:%M:%S')
        for label in labels:
            label_start_time, label_end_time, mode = label
            label_start_time_dt = datetime.datetime.strptime(standardize_date_format(label_start_time), '%Y-%m-%d %H:%M:%S')
            label_end_time_dt = datetime.datetime.strptime(standardize_date_format(label_end_time), '%Y-%m-%d %H:%M:%S')
            if label_start_time_dt == start_time_dt and label_end_time_dt == end_time_dt:
                return mode
        return None


def main():
    BATCH_SIZE = 10000 
    program = None
    try:
        program = ExampleProgram()
        program.drop_table(table_name="TrackPoint")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="User")   
        program.create_tables()
        user_id = "020"
        user_path = os.path.join("dataset", "dataset", "Data", user_id)
        labels = program.read_labels(user_path)
        has_labels = len(labels) > 0
        program.insert_user(user_id, has_labels)

        trajectory_path = os.path.join(user_path, "Trajectory")
        plt_files = [some_file for some_file in os.listdir(trajectory_path) if some_file.endswith('.plt')]
        print("start reading trackpoints")
        for plt_file in plt_files:
            trackpoints = program.read_trackpoints(os.path.join(trajectory_path, plt_file))

            if not trackpoints:
                continue

            start_time = standardize_date_format(trackpoints[0][5] + " " + trackpoints[0][6])
            end_time = standardize_date_format(trackpoints[-1][5] + " " + trackpoints[-1][6])

            mode = program.get_mode_from_labels(labels, start_time, end_time)
            
            activity_id = program.insert_activity(user_id, mode, start_time.replace("/", "-"), end_time.replace("/", "-"))
            
            # Use batch insert for trackpoints
            trackpoint_data = [(activity_id, point[0], point[1], 0, point[4], f"{point[5]} {point[6]}") for point in trackpoints]
            program.insert_trackpoints_batch(trackpoint_data)
        print("finished reading trackpoints")
        program.show_tables()
        program.fetch_data("User") 
        program.fetch_data("Activity")

        program.drop_table(table_name="TrackPoint")
        program.drop_table(table_name="Activity")
        program.drop_table(table_name="User")    
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
