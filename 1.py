from DbConnector import DbConnector
from tabulate import tabulate
import os
import datetime  

class GetCount:

    def __init__(self):
            self.connection = DbConnector()
            self.db_connection = self.connection.db_connection
            self.cursor = self.connection.cursor
    
    def count_table(self, table_name):
        query = "SELECT COUNT(*) FROM %s"
        self.cursor.execute(query % table_name)
        count = self.cursor.fetchone()[0]
        return count
    

def main():
    program = None
    try:
        program = GetCount()
        tables = ['User', 'Activity', 'TrackPoint']
        for table in tables:
            count = program.count_table(table_name=table)
            print(f"The number of records in {table} is: {count}")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()