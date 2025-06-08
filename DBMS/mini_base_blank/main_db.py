# ------------------------------------------------
# main_db.py
# Author: Jingyu Han   hjymail@163.com
# Modified by: Xinjian Zhang   278254081@qq.com
# Modified by: WuShuCheng  2396878284@qq.com
# Modified by: Ruizhe Yang   419198812@qq.com
# ------------------------------------------------
# This is the main loop of the database management system
# Provides user interface for database operations
# ------------------------------------------------

import schema_db      # Module to process table schema
import storage_db     # Module to process data storage
import query_plan_db  # Module for SQL clause processing
import lex_db         # Module for lexical analysis
import parser_db      # Module for syntax analysis
import common_db      # Global variables, functions, and constants
import log_db         # Module for transaction logging
import index_manager  # Module for index management

# User interface prompt string
PROMPT_STR = '''
 +-----------------------------------------+
 |               MENU OPTIONS              |
 +-----------------------------------------+
 | 1: Add a new table structure or data    |
 | 2: Delete a table structure and data    |
 | 3: View a table structure and data      |
 | 4: Delete all tables and data           |
 | 5: Delete a row by field keyword        |
 | 6: Update a row by field keyword        |
 | 7: SQL                                  |
 | 8: View log files                       |
 | 9: Index Management                     |
 | .: Quit                                 |
 +-----------------------------------------+
 Input your choice: '''  # the prompt string for user input

# ------------------------------------------------
# Main program loop
# Handles user input and executes corresponding operations
# Input:
#       None (reads from user input)
# Output:
#       None (interactive program)
# ------------------------------------------------
def main():
    # Initialize schema object for table metadata management
    schema_obj = schema_db.Schema(debug=False)
    data_obj = None
    choice = input(PROMPT_STR)

    while True:
        if choice == '1':  # Add new table structure or data
            table_name = input('Please enter your new table name: ')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            
            # Check if table already exists
            if table_name.strip() not in schema_obj.get_table_name_list():
                # Create new table
                data_obj = storage_db.Storage(table_name, debug=False)
                insert_field_list = data_obj.getFieldList()
                schema_obj.appendTable(table_name, insert_field_list)
                print('OK!')
            else:
                # Insert data into existing table
                data_obj = storage_db.Storage(table_name, debug=False)
                record = []
                field_list = data_obj.getFieldList()
                
                # Prompt for field values
                for field in field_list:
                    field_name = field[0].decode('utf-8').strip() if isinstance(field[0], bytes) else str(field[0]).strip()
                    prompt = f'Input field name: {field_name}  field type: {field[1]}  field maximum length: {field[2]}\n-->'
                    record.append(input(prompt))
                
                if data_obj.insert_record(record):
                    print('OK!')
                else:
                    print('Wrong input!')
                del data_obj
            choice = input(PROMPT_STR)

        elif choice == '2':  # Delete table structure and data
            table_name = input('Please input the name of the table to be deleted: ')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            
            if schema_obj.find_table(table_name.strip()):
                if schema_obj.delete_table_schema(table_name):
                    data_obj = storage_db.Storage(table_name, debug=False)
                    data_obj.delete_table_data(table_name.strip())
                    print('OK!')
                    del data_obj
                else:
                    print('The deletion from schema file failed')
            else:
                print(f"There is no table {table_name.decode('utf-8')} in the schema file")
            choice = input(PROMPT_STR)

        elif choice == '3':  # View table structure and data
            print("Current tables:")
            for table in schema_obj.headObj.tableNames:
                if isinstance(table[0], bytes):
                    print(table[0].decode('utf-8').strip())
                else:
                    print(str(table[0]).strip())
            
            table_name = input('Please input the name of the table to be displayed: ')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            
            if table_name.strip():
                if schema_obj.find_table(table_name.strip()):
                    data_obj = storage_db.Storage(table_name, debug=True)  # Show debug info only here
                    data_obj.show_table_data()
                    print('OK!')
                    del data_obj
                else:
                    print('Table name is None')
            choice = input(PROMPT_STR)

        elif choice == '4':  # Delete all table structures and data
            table_name_list = list(schema_obj.get_table_name_list())
            for table_name in table_name_list:
                table_name.strip()
                if table_name:
                    data_obj = storage_db.Storage(table_name, debug=False)
                    data_obj.delete_table_data(table_name.strip())
                    del data_obj
            schema_obj.deleteAll()
            print('OK!')
            choice = input(PROMPT_STR)

        elif choice == '5':  # Delete row by field keyword
            table_name = input('Please input the name of the table to delete from: ')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            
            field_input = input('Please input the field name and keyword (fieldname:keyword): ')
            if ':' in field_input:
                field_name, keyword = field_input.split(':', 1)
                data_obj = storage_db.Storage(table_name, debug=False)
                data_obj.delete_record_by_field(field_name.strip(), keyword.strip())
                print('OK!')
                del data_obj
            else:
                print("Input format error. Please use fieldname:keyword")
            choice = input(PROMPT_STR)

        elif choice == '6':  # Update row by field keyword
            table_name = input('Please input the name of the table to update: ')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            
            field_input = input('Please input the field name and keyword (fieldname:keyword): ')
            if ':' in field_input:
                field_name, old_value = field_input.split(':', 1)
                new_value = input('Please input the new value: ').strip()
                data_obj = storage_db.Storage(table_name, debug=False)
                data_obj.update_record_by_field(field_name.strip(), old_value.strip(), field_name.strip(), new_value)
                print('OK!')
                del data_obj
            else:
                print("Input format error. Please use fieldname:keyword")
            choice = input(PROMPT_STR)

        elif choice == '7':  # Process SQL statements
            print('#' + '-'*30 + ' SQL QUERY BEGIN ' + '-'*30 + '#')
            sql_str = input('Please enter SQL statement: ')
            try:
                lex_db.set_lex_handle()        # Initialize lexer
                parser_db.set_parser_handle()  # Initialize parser
                common_db.global_lexer.input(sql_str)
                common_db.global_syn_tree = common_db.global_parser.parse(lexer=common_db.global_lexer)
                if common_db.global_syn_tree:
                    query_plan_db.execute_sql_statement(schema_obj)
                else:
                    print("Failed to parse SQL statement")
            except Exception as e:
                print(f"Error processing SQL: {e}")
            print('#' + '-'*30 + ' SQL QUERY END ' + '-'*32 + '#')
            choice = input(PROMPT_STR)

        elif choice == '8':  # View log files
            before_logs = log_db.LogManager.read_log_file(log_db.BEFORE_IMAGE_FILE)
            after_logs = log_db.LogManager.read_log_file(log_db.AFTER_IMAGE_FILE)
            active_logs = log_db.LogManager.read_log_file(log_db.ACTIVE_TX_FILE)
            commit_logs = log_db.LogManager.read_log_file(log_db.COMMIT_TX_FILE)
            
            print("Before Image Log:")
            for log in before_logs:
                print(f'{log.strip()}')
            print("After Image Log:")
            for log in after_logs:
                print(f'{log.strip()}')
            print("Active Transaction Log:")
            for log in active_logs:
                print(f'{log.strip()}')
            print("Commit Transaction Log:")
            for log in commit_logs:
                print(f'{log.strip()}')
            choice = input(PROMPT_STR)
            
        elif choice == '9':  # Index management
            index_manager.handle_index_management(schema_obj)
            choice = input(PROMPT_STR)

        elif choice == '.':  # Quit the program
            print('Main loop finishes')
            del schema_obj
            break

        else:
            print('Wrong input, please try again!')
            choice = input(PROMPT_STR)

if __name__ == '__main__':
    main()