import csv
import os

class MyDB:
    def __init__(self, db_root_directory="dataset"):
        self.db_root_directory = db_root_directory
        if not os.path.exists(db_root_directory):
            print("Error: Database root directory does not exist.")
            exit(1)

    def _table_path(self, table_identifier):
        # table_identifier can be 'table_name' or 'folder/table_name'
        return os.path.join(self.db_root_directory, f"{table_identifier}.csv")

    def insert_into(self, table_name, row_data):
        if not os.path.exists(self._table_path(table_name)):
            print("Error: Table does not exist.")
            return

        with open(self._table_path(table_name), 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(row_data)
        print("Data inserted.")

    def read_csv_in_chunks(self, file_path, chunk_size=50):
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            chunk = []
            for i, row in enumerate(reader):
                if i % chunk_size == 0 and i > 0:
                    yield chunk
                    chunk = []
                chunk.append(row)
            yield chunk

    def select_from(self, table_name, columns=None, distinct=False, where_clause=None):
        if not os.path.exists(self._table_path(table_name)):
            print("Error: Table does not exist.")
            return

        first_chunk = True
        col_indices = None
        seen_values = set()  # To track distinct values

        for chunk in self.read_csv_in_chunks(self._table_path(table_name)):
            if first_chunk:
                header = chunk[0]
                if columns:
                    try:
                        col_indices = [header.index(col) for col in columns.split(',')]
                    except ValueError:
                        print(f"Error: Column '{columns}' not found in table '{table_name}'.")
                        return
                else:
                    col_indices = range(len(header))
                if where_clause:
                    try:
                        where_col, where_val = where_clause.split(" = ")
                        where_col_index = header.index(where_col)
                    except ValueError:
                        print(f"Error: Invalid where clause '{where_clause}'.")
                        return
                first_chunk = False
                data_start_index = 1
            else:
                data_start_index = 0

            for row in chunk[data_start_index:]:
                if where_clause and row[where_col_index] != where_val.strip("'"):
                    continue
                row_data = tuple(row[i] for i in col_indices)
                if not distinct or row_data not in seen_values:
                    if len(row_data) == 1:  # If only one column is selected
                        print(row_data[0])  # Print the single value directly
                    else:
                        print(row_data)  # Print the tuple for multiple columns
                    if distinct:
                        seen_values.add(row_data)

    def handle_query(self, query):
        query_parts = query.split()
        command = query_parts[0].lower()

        if command == "insert" and query_parts[1].lower() == "into":
            table_name = query_parts[2]
            row_data = query_parts[3:]
            self.insert_into(table_name, row_data)

        elif command == "select":
            distinct = "distinct" in query_parts
            if distinct:
                query_parts.remove("distinct")

            columns = query_parts[1]
            from_index = query_parts.index("from")
            table_name = query_parts[from_index + 1]

            where_clause = None
            if "where" in query_parts:
                where_index = query_parts.index("where")
                where_clause = ' '.join(query_parts[where_index + 1:])

            self.select_from(table_name, columns, distinct, where_clause)
        else:
            print("Query not recognized.")

def main():
    mydb = MyDB()
    query = ""

    print("Enter your query and end with '!' to execute, enter exit! to exit console")
    while True:
        line = input("MyDB > " if not query else "... > ")
        query += line + " "
        if query.strip().endswith('!'):
            query = query.strip()[:-1]  # Remove the '!' from the end
            if query.lower() == "exit":
                break
            mydb.handle_query(query)
            query = ""

if __name__ == "__main__":
    main()
