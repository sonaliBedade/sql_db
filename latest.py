import os
import csv
import sys
import shutil

class MyDB:
    def __init__(self, db_root_directory="dataset"):
        self.db_root_directory = db_root_directory
        if not os.path.exists(db_root_directory):
            print("Error: Database root directory does not exist.")
            exit(1)

    def create_db(self, dbname):
        """Creates a new database (folder) with the specified name."""
        db_path = os.path.join(self.db_root_directory, dbname)
        if not os.path.exists(db_path):
            os.makedirs(db_path)
            print(f"Database '{dbname}' created successfully.")
        else:
            print(f"Database '{dbname}' already exists.")

    def use_db(self, dbname):
        """Switches to the specified database (folder)."""
        db_path = os.path.join(self.db_root_directory, dbname)
        if os.path.exists(db_path):
            self.current_db = db_path
            print(f"Switched to database '{dbname}'.")
        else:
            print(f"Database '{dbname}' does not exist.")

    def create_table(self, table_name):
        """Creates a new table (CSV file) in the current database."""
        if not self.current_db:
            print("No database selected. Use 'use db [dbname]!' to select a database.")
            return

        table_path = os.path.join(self.current_db, f"{table_name}.csv")
        if not os.path.exists(table_path):
            with open(table_path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                # Optional: Write headers to the CSV file if needed
                # writer.writerow(['Column1', 'Column2', ...])
            print(f"Table '{table_name}' created in database '{os.path.basename(self.current_db)}'.")
        else:
            print(f"Table '{table_name}' already exists in database '{os.path.basename(self.current_db)}'.")

    def drop_table(self, table_name):
        """Drops (deletes) a table from the current database."""
        if not self.current_db:
            print("No database selected. Use 'use db [dbname]!' to select a database.")
            return

        table_path = os.path.join(self.current_db, f"{table_name}.csv")
        if os.path.exists(table_path):
            os.remove(table_path)
            print(f"Table '{table_name}' has been removed from database '{os.path.basename(self.current_db)}'.")
        else:
            print(f"Table '{table_name}' does not exist in the database '{os.path.basename(self.current_db)}'.")

    def drop_database(self, db_name):
        """Drops (deletes) the specified database and its directory."""
        db_path = os.path.join(self.db_root_directory, db_name)
        if os.path.exists(db_path) and os.path.isdir(db_path):
            shutil.rmtree(db_path)
            print(f"Database '{db_name}' has been deleted.")

            # Reset current database if it was the one deleted
            if self.current_db == db_path:
                self.current_db = self.db_root_directory
        else:
            print(f"Database '{db_name}' does not exist.")

    def insert_into_table(self, table_name, values):
        """Inserts data into a specified table."""
        if not self.current_db or self.current_db == self.db_root_directory:
            print("No database selected. Use 'use db [dbname]!' to select a database.")
            return

        table_path = os.path.join(self.current_db, f"{table_name}.csv")
        if not os.path.exists(table_path):
            print(f"Table '{table_name}' does not exist.")
            return

        with open(table_path, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(values)
        print(f"Data added to table '{table_name}'.")

    def _table_path(self, table_identifier):
        return os.path.join(self.db_root_directory, f"{table_identifier}.csv")
    
    def estimate_row_size(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            sample_rows = [next(reader) for _ in range(10)]
            sample_size = sum(sys.getsizeof(row) for row in sample_rows)
            avg_row_size = sample_size // len(sample_rows)
            return avg_row_size

    def chunk_csv(self, table_name, memory_limit=1e6):  # 1MB default limit
        file_path = self._table_path(table_name)
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} does not exist.")
            return

        row_size = self.estimate_row_size(file_path)
        rows_per_chunk = int(memory_limit // row_size)

        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            chunk_count = 0
            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) >= rows_per_chunk:
                    self._save_chunk(table_name, chunk_count, chunk)
                    chunk = []
                    chunk_count += 1
            
            if chunk:  # Save any remaining rows in the last chunk
                self._save_chunk(table_name, chunk_count, chunk)

    def _save_chunk(self, table_name, chunk_number, chunk_data):
        chunk_file = self._table_path(f"{table_name}_chunk{chunk_number}")
        with open(chunk_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(chunk_data)
        print(f"Chunk {chunk_number} saved for {table_name}.")

    def select_from(self, table_name, columns, distinct=False, where_clause=None, memory_limit=1e6):
        file_path = self._table_path(table_name)
        if not os.path.exists(file_path):
            print("Error: Table does not exist.")
            return
    
        # Handling aggregate functions
        aggregate_function = None
        column_to_aggregate = None
        count, sum_value, min_value, max_value = 0, 0, None, None

        select_all_columns = columns == ['*']
        
        columns_string = ', '.join(columns) if isinstance(columns, list) else columns
        
        if 'min(' in columns_string or 'max(' in columns_string or 'avg(' in columns_string or 'sum(' in columns_string or 'count(' in columns_string:
            if 'min(' in columns_string:
                aggregate_function = 'min'
            elif 'max(' in columns_string:
                aggregate_function = 'max'
            elif 'avg(' in columns_string:
                aggregate_function = 'avg'
            elif 'sum(' in columns_string:
                aggregate_function = 'sum'
            else:
                aggregate_function = 'count'
            column_to_aggregate = columns_string.split('(')[1].split(')')[0]
    
        seen_values = set()
        memory_usage = 0
        results = []       

        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            rows_list = list(reader)  # Convert DictReader to a list

            if select_all_columns:
                columns = reader.fieldnames
                max_col_widths = {col: len(col) for col in columns}
            else:
                max_col_widths = {col: len(col) for col in columns}

            for row in rows_list:
                if where_clause and not self._evaluate_where(row, reader.fieldnames, where_clause):
                    continue

                
                # Aggregate function handling
                if aggregate_function:
                    try:
                        value = float(row[column_to_aggregate])
                        if aggregate_function == 'count':
                            count += 1
                        elif aggregate_function == 'avg':
                            sum_value += value
                            count += 1
                        elif aggregate_function == 'min':
                            min_value = value if min_value is None else min(min_value, value)
                        elif aggregate_function == 'max':
                            max_value = value if max_value is None else max(max_value, value)
                        else:
                            sum_value += value
                    except (ValueError, KeyError):
                        continue

                else:
                    row_data = tuple(row[col] for col in columns if col in row)
                    row_memory = sum(sys.getsizeof(item) for item in row_data)

                    if memory_usage + row_memory > memory_limit:
                        break  # Stop processing if memory limit is exceeded
                
                    if not distinct or row_data not in seen_values:
                        results.append(row_data)
                        memory_usage += row_memory
                        if distinct:
                            seen_values.add(row_data)
                        for i, col in enumerate(columns):
                            if row_data and i < len(row_data):
                                max_col_widths[col] = max(max_col_widths[col], len(str(row_data[i])))

        # Aggregate function results
        if aggregate_function == 'avg':
            avg_value = sum_value / count if count > 0 else 0
            print(f"Avg: {avg_value}")
        elif aggregate_function == 'count':
            print(f"Count: {count}")
        elif aggregate_function == 'sum':
            print(f"Sum: {sum_value}")
        elif aggregate_function == 'min':
            print(f"Min: {min_value}")
        elif aggregate_function == 'max':
            print(f"Max: {max_value}")
        else:
            self.print_table(columns, max_col_widths, results, memory_usage)    


    def print_table(self, columns, max_col_widths, data_rows, memory_usage):
        border_line = '+' + '+'.join('-' * (max_col_widths[col] + 2) for col in columns) + '+'
        print(border_line)
        header_line = '| ' + ' | '.join(f"{col:{max_col_widths[col]}}" for col in columns) + ' |'
        print(header_line)
        print(border_line)

        for row_data in data_rows:
            row_line = '| ' + ' | '.join(f"{data:{max_col_widths[col]}}" for data, col in zip(row_data, columns)) + ' |'
            print(row_line)

        print(border_line)
        print(f"Estimated memory usage for query result: {memory_usage} bytes")

    '''finalONEdef _evaluate_condition(self, row, header, condition):
        parts = condition.split()

        if len(parts) < 3:
            raise ValueError("Invalid condition format")

        column, operator, *value_parts = parts
        value = ' '.join(value_parts).strip("'")

        if column in header:
            row_value = row[column]

        # Handle different operators
            if operator in ('=', '=='):
                return row_value == value
            elif operator == '>=':
                return row_value >= value
            elif operator == '<=':
                return row_value <= value
            elif operator == '>':
                return row_value > value
            elif operator == '<':
                return row_value < value
            elif operator in ('!=', '<>'):
                return row_value != value
            else:
                raise ValueError(f"Unsupported operator: {operator}")
        else:
            raise ValueError(f"Column {column} not found in header")finalONE'''

    def _evaluate_condition(self, row, header, condition):
        parts = condition.split()

        if len(parts) < 3:
            raise ValueError("Invalid condition format")

        column, operator, *value_parts = parts
        value = ' '.join(value_parts).strip("'")

        if column in header:
            row_value = row[column]

        # Handle different operators
            if operator in ('=', '=='):
                return row_value == value
            elif operator == '>=':
                return row_value >= value
            elif operator == '<=':
                return row_value <= value
            elif operator == '>':
                return row_value > value
            elif operator == '<':
                return row_value < value
            elif operator in ('!=', '<>'):
                return row_value != value
            else:
                raise ValueError(f"Unsupported operator: {operator}")
        else:
            raise ValueError(f"Column {column} not found in header")

    '''finalOnedef _evaluate_where(self, row, header, where_clause):
        try:
        # Incrementing a counter to track method calls
            self._evaluate_where_counter = getattr(self, '_evaluate_where_counter', 0) + 1

        # Split the where_clause into individual conditions
            conditions = where_clause.split(',')

        # Check if 'or' or 'and' is present in the where_clause
            if ' or ' in where_clause:
                return any(self._evaluate_condition(row, header, condition) for condition in conditions)
            elif ' and ' in where_clause:
                return all(self._evaluate_condition(row, header, condition) for condition in conditions)

        # Ensure each condition has at least three parts: column, operator, and value
            valid_conditions = []
            for condition in conditions:
                parts = condition.split('=')
                if len(parts) == 2:
                    column, value = parts[0].strip(), parts[1].strip().strip("'")
                    valid_conditions.append(f"{column} = {value}")

        # Evaluate each valid condition
            return all(self._evaluate_single_condition(row, header, condition) for condition in valid_conditions)

        except ValueError as e:
            print(f"Error in where clause: {e}")
            return FalsefinalOne'''

    def _evaluate_where(self, row, header, where_clause):
        try:
        # Incrementing a counter to track method calls
            self._evaluate_where_counter = getattr(self, '_evaluate_where_counter', 0) + 1

        # Split the where_clause into individual conditions
            conditions = where_clause.split(' or ')

        # Check if 'or' is present in the where_clause
            if ' or ' in where_clause:
                return any(self._evaluate_condition(row, header, condition) for condition in conditions)

            conditions = where_clause.split(' and ')

            if ' and ' in where_clause:
                return all(self._evaluate_single_condition(row, header, condition) for condition in conditions)

        # Ensure each condition has at least three parts: column, operator, and value
            valid_conditions = []
            for condition in conditions:
                parts = condition.split('=')
                if len(parts) == 2:
                    column, value = parts[0].strip(), parts[1].strip().strip("'")
                    valid_conditions.append(f"{column} = {value}")

        # Evaluate each valid condition
            return all(self._evaluate_single_condition(row, header, condition) for condition in valid_conditions)

        except ValueError as e:
            print(f"Error in where clause: {e}")
            return False


    def _evaluate_like_condition(self, row, header, condition):
        parts = condition.split(' like ')
        if len(parts) != 2:
            raise ValueError("Invalid LIKE condition format")

        column, pattern = parts[0].strip(), parts[1].strip().strip("'")
        if column not in header:
            raise ValueError(f"Column {column} not found in header")

        column_index = header.index(column)
        row_value = row[column_index]

        return self._match_like_pattern(row_value, pattern)

    def _match_like_pattern(self, value, pattern): 
        if not pattern:
            return not value

    # If the pattern starts with '%', find the first occurrence of the next part in value
        if pattern[0] == '%':
            if len(pattern) == 1:
                return True  # Match any value
            else:
                next_part = pattern[1]
                if next_part == '%':
                # Handle consecutive '%' characters
                    return self._match_like_pattern(value, pattern[1:])
                else:
                # Find the next part in value
                    for i in range(len(value)):
                        if self._match_like_pattern(value[i:], pattern[1:]):
                            return True
                    return False
        elif pattern[0] == '_':
        # If the pattern starts with '_', match exactly one character
            return len(value) > 0 and self._match_like_pattern(value[1:], pattern[1:])
        else:
        # Match the first character and continue
            return len(value) > 0 and value[0] == pattern[0] and self._match_like_pattern(value[1:], pattern[1:])

    def _split_like_pattern(self, pattern):
        """Splits the LIKE pattern into a list of characters and wildcards."""
        parts = []
        i = 0
        while i < len(pattern):
            if pattern[i] in ['%', '_']:
                parts.append(pattern[i])
            else:
                parts.append(pattern[i])
            i += 1
        return parts

    def _evaluate_single_condition(self, row, header, condition):
        # Split condition into parts
        parts = condition.split()

        if len(parts) < 3:
            raise ValueError("Invalid condition format")

        column, operator, value = parts[0], parts[1], ' '.join(parts[2:])
        value = value.strip("'")

        if column in header:
            row_value = row[column]

            # Handle different operators
            if operator in ('=', '=='):
                return row_value == value
            elif operator == '>=':
                return row_value >= value
            elif operator == '<=':
                return row_value <= value
            elif operator == '>':
                return row_value > value
            elif operator == '<':
                return row_value < value
            elif operator in ('!=', '<>'):
                return row_value != value
            else:
                raise ValueError(f"Unsupported operator: {operator}")
        else:
            raise ValueError(f"Column {column} not found in header")

    def handle_query(self, query):
        query = query.strip()
        if query.endswith('!'):
            query = query[:-1]

        query_parts = query.split()
        command = query_parts[0].lower()

        if command == "new" and query_parts[1].lower() == "db":
            dbname = query_parts[2]
            self.create_db(dbname)

        elif command == "use" and query_parts[1].lower() == "db":
            dbname = query_parts[2]
            self.use_db(dbname)

        elif command == "new" and query_parts[1].lower() == "table":
            table_name = query_parts[2]
            self.create_table(table_name)

        elif command == "rem" and query_parts[1].lower() == "table":
            table_name = query_parts[2]
            self.drop_table(table_name)

        elif command == "trash" and query_parts[1].lower() == "db":
            db_name = query_parts[2]
            self.drop_database(db_name)

        elif command == "add" and query_parts[1].lower() == "in":
            table_name = query_parts[2]
            values_part = query.split('as')[1].strip().strip('()')
            values = [value.strip() for value in values_part.split(',')]
            self.insert_into_table(table_name, values)

        elif command == "get":
            distinct = "once" in query.lower()
            from_index = query_parts.index("->")
            where_index = query_parts.index("that") if "that" in query_parts else len(query_parts)

            column_part = query.split("get")[1].split("->")[0].strip()
            if distinct:
                column_part = column_part.replace("once", "").strip()
            columns = [col.strip() for col in column_part.split(",")]

            table_name = query_parts[from_index + 1]
            where_clause = ' '.join(query_parts[where_index + 1:]) if "that" in query_parts else None

            self.select_from(table_name, columns, distinct, where_clause)

        else:
            print("Query not recognized.")
    
def main():
    mydb = MyDB()
    query = ""

    print("Enter your query and end with '!' to execute, enter exit! to exit console")
    while True:
        line = input("MyDB> " if not query else "... > ")
        query += line + " "
        if query.strip().endswith('!'):
            query = query.strip()[:-1]
            if query.lower() == "exit":
                break
            mydb.handle_query(query)
            query = ""

if __name__ == "__main__":
    main() 
