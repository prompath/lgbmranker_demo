import csv
from glob import glob
from pathlib import Path
import sqlite3

from metaflow import FlowSpec, Parameter, step


class DBCreationFlow(FlowSpec):
    db_dir = Parameter(
            "db_dir",
            help="Directory to create SQLite datebase files",
            default="./db/"
            )
    data_dir = Parameter(
            "data_dir",
            help="Directory containing the CSV files",
            default="./data/"
            )
    
    @step
    def start(self):
        self.db_dir_path = Path(self.db_dir).resolve()
        self.db_dir_path.mkdir(parents=True, exist_ok=True)
        self.data_dir_path = Path(self.data_dir).resolve()
        print("Creating SQLite databases...")
        print(f"-> Reading CSV files from {self.data_dir_path}")
        print(f"-> Directory to create database files: {self.db_dir_path}")
        self.next(self.create_db)

    @step
    def create_db(self):
        csv_files = glob(self.data_dir_path / "*.csv")
        for file in csv_files:
            db_path = self.db_dir_path / file.stem
            print(f"Creating database at {db_path}")
            con = sqlite3.connect(db_path)
            cursor = con.cursor()
            with open(file, "r") as f:
                data = csv.DictReader(f)
                table_name = file.stem
                fieldnames = tuple(data.fieldnames)
                cursor.execute(f"CREATE TABLE {table_name} {fieldnames};")
                for row in data:
                    cursor.execute(f"INSERT INTO {table_name} VALUES {tuple(row)};")
            con.commit()
            con.close()
        print(f"Database creation done")
        self.next(self.end)

    @step
    def end(self):
        pass

