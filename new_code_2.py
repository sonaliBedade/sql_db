import os
import csv
import sys

class MyDB:
    def __init__(self, db_root_directory="dataset"):
        self.db_root_directory = db_root_directory
        if not os.path.exists(db_root_directory):
            print("Error: Database root directory does not exist.")
            exit(1)

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

        max_col_widths = {col: len(col) for col in columns}
        seen_values = set()
        memory_usage = 0
        header_processed = False
        results = []

        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            for row in reader:
                if not header_processed:
                    header = row
                    
                    col_indices = [header.index(col.strip()) for col in columns if col.strip() in header]
                    if len(col_indices) != len(columns):
                        print("Error: One or more columns not found.")
                        return
                    header_processed = True
                    continue

                if where_clause and not self._evaluate_where(row, header, where_clause):
                    continue

                row_data = tuple(row[i] for i in col_indices)
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

    def _evaluate_where(self, row, header, where_clause):
        try:
            and_splits = where_clause.split(' and ')
            all_conditions = []

            for and_cond in and_splits:
                or_splits = and_cond.split(' or ')
                or_results = [self._evaluate_condition(row, header, cond.strip()) for cond in or_splits]
                if any(or_results):
                    all_conditions.append(True)
                else:
                    all_conditions.append(False)

            return all(all_conditions)
        except ValueError as e:
            print(f"Error in where clause: {e}")
            return False

    def _evaluate_condition(self, row, header, condition):
        # Check for LIKE condition
        if ' like ' in condition.lower():
            return self._evaluate_like_condition(row, header, condition)
        else:
            return self._evaluate_single_condition(row, header, condition)
    
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
            column_index = header.index(column)
            row_value = row[column_index]

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

        if command == "insert" and query_parts[1].lower() == "into":
            table_name = query_parts[2]
            row_data = query_parts[3:]
            self.insert_into(table_name, row_data)

        elif command == "select":
            distinct = "distinct" in query.lower()
            from_index = query_parts.index("from")
            where_index = query_parts.index("where") if "where" in query_parts else len(query_parts)

            column_part = query.split("select")[1].split("from")[0].strip()
            if distinct:
                column_part = column_part.replace("distinct", "").strip()
            columns = [col.strip() for col in column_part.split(",")]

            table_name = query_parts[from_index + 1]
            where_clause = ' '.join(query_parts[where_index + 1:]) if "where" in query_parts else None

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
