import csv
from pathlib import Path
import sqlite3

from metaflow import FlowSpec, Parameter, step


class DBCreationFlow(FlowSpec):
    db_path = Parameter(
        "db_path",
        help="Directory to create SQLite datebase file",
        default="./db/anime.db",
    )
    data_dir = Parameter(
        "data_dir", help="Directory containing the CSV files", default="./data/"
    )

    @step
    def start(self):
        self.db_path_ = Path(self.db_path).resolve()
        self.data_dir_path = Path(self.data_dir).resolve()
        print("Creating SQLite databases...")
        self.next(self.create_db)

    @step
    def create_db(self):
        print(f"-> Reading CSV files from {self.data_dir_path}")
        csv_files = self.data_dir_path.glob("*.csv")
        print(f"-> Creating database at {self.db_path_}")
        con = sqlite3.connect(self.db_path_)
        cursor = con.cursor()
        for file in csv_files:
            with open(file, "r") as f:
                data = csv.DictReader(f)
                table_name = file.stem
                fieldnames = tuple(data.fieldnames)
                placeholders = ", ".join(["?" for _ in fieldnames])
                print(f"Creating table `{table_name}`...")
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                cursor.execute(f"CREATE TABLE {table_name} {fieldnames};")
                for row in data:
                    row_values = tuple(row.values())
                    cursor.execute(
                        f"INSERT INTO {table_name} VALUES ({placeholders});", row_values
                    )
        con.commit()
        con.close()
        print("Database creation done")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    DBCreationFlow()
