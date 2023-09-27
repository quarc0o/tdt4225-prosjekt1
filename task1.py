from DbConnector import DbConnector
from tabulate import tabulate
import os
    


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
        with open(labels_path, 'r') as file:
            lines = file.readlines()[1:]  
        activities = [line.strip().split('\t') for line in lines]
        return activities

    def read_trajectory_files(self, trajectory_path):
        files = [os.path.join(trajectory_path, f) for f in os.listdir(trajectory_path) if f.endswith('.plt')]
        all_points = []
        for file in files:
            with open(file, 'r') as f:
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

    def insert_trackpoint(self, activity_id, lat, lon, altitude, date_days, date_time):
        query = """INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) 
                VALUES (%s, %s, %s, %s, %s, %s)"""
        self.cursor.execute(query, (activity_id, lat, lon, altitude, date_days, date_time))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
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

        for label in labels:
            start_time, end_time, mode = label
            activity_id = program.insert_activity(user_id, mode, start_time.replace("/", "-"), end_time.replace("/", "-"))

            trajectory_path = os.path.join(user_path, "Trajectory")
            trackpoints = program.read_trajectory_files(trajectory_path)
            print("Starting trackpoint insertion...")
            for point in trackpoints:
                print("trackpoint: ", point)
                lat, lon, _, _, date_days, date, time = point
                datetime_str = f"{date} {time}"
                program.insert_trackpoint(activity_id, lat, lon, 0, date_days, datetime_str)
            print("Finished trackpoint insertion.")
        program.show_tables()
        program.fetch_data("User") 
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
