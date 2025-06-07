# -----------------------
# main_db.py
# author: Jingyu Han   hjymail@163.com
# modified by: Xinjian Zhang   278254081@qq.com
# -----------------------------------
# This is the main loop of the program
# ---------------------------------------
import schema_db  # the module to process table schema
import storage_db  # the module to process the storage of instance
import query_plan_db  # for SQL clause of which data is stored in binary format
import lex_db  # for lex, where data is stored in binary format
import parser_db  # for yacc, where ddata is tored in binary format
import common_db  # the global variables, functions, constants in the program
import query_plan_db  # construct the query plan and execute it

PROMPT_STR = '''
 +-----------------------------------------+
 |               MENU OPTIONS              |
 +-----------------------------------------+
 | 1: Add a new table structure and data   |
 | 2: Delete a table structure and data    |
 | 3: View a table structure and data      |
 | 4: Delete all tables and data           |
 | 5: Delete a row by field keyword        |
 | 6: Update a row by field keyword        |
 | 7: SQL                                  |
 | 9: Index Management                     |
 | .: Quit                                 |
 +-----------------------------------------+
 Input your choice: '''  # the prompt string for user input

# --------------------------
# the main loop, which needs further implementation
# ---------------------------
def main():
    # main loops for the whole program
    print('main function begins to execute')
    # The instance data of table is stored in binary format, which corresponds to chapter 2-8 of textbook
    schemaObj = schema_db.Schema(debug=True)  # to create a schema object, which contains the schema of all tables(增加了调试选项)
    dataObj = None
    choice = input(PROMPT_STR)

    while True:
        if choice == '1':  # add a new table and lines of data
            tableName = input('please enter your new table name:')
            if isinstance(tableName, str):
                tableName = tableName.encode('utf-8')
            # tableName not in all.sch
            insertFieldList = []
            if tableName.strip() not in schemaObj.get_table_name_list():
                # Create a new table
                dataObj = storage_db.Storage(tableName)
                insertFieldList = dataObj.getFieldList()
                schemaObj.appendTable(tableName, insertFieldList)  # add the table structure
            else:
                dataObj = storage_db.Storage(tableName)
                # to the students: The following needs to be further implemented (many lines can be added)
                record = []
                Field_List = dataObj.getFieldList()
                for x in Field_List:
                    s = 'Input field name is: ' + str(x[0].decode('utf-8').strip()) + '  field type is: ' + str(x[1]) + '  field maximum length is: ' + str(x[2]) + '\n' +'-->'
                    record.append(input(s))
                if dataObj.insert_record(record):  # add a row
                    print('OK!')
                else:
                    print('Wrong input!')
                del dataObj
            choice = input(PROMPT_STR)

        elif choice == '2':  # delete a table from schema file and data file
            table_name = input('please input the name of the table to be deleted:')
            if isinstance(table_name,str):
                table_name=table_name.encode('utf-8')
            if schemaObj.find_table(table_name.strip()):
                if schemaObj.delete_table_schema(table_name):  # delete the schema from the schema file
                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.delete_table_data(table_name.strip())  # delete table content from the table file
                    del dataObj
                else:
                    print('the deletion from schema file fail')
            else:
                print(f"there is no table {table_name.decode('utf-8')} in the schema file")
            choice = input(PROMPT_STR)

        elif choice == '3':  # view the table structure and all the data
            print("Current tables:")
            for t in schemaObj.headObj.tableNames:
                if isinstance(t[0], bytes):
                    print(t[0].decode('utf-8').strip())
                else:
                    print(str(t[0]).strip())
            table_name = input('please input the name of the table to be displayed:')
            if isinstance(table_name,str):
                table_name=table_name.encode('utf-8')
            if table_name.strip():
                if schemaObj.find_table(table_name.strip()):
                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.show_table_data()  # view all the data of the table
                    del dataObj
                else:
                    print('table name is None')
            choice = input(PROMPT_STR)

        elif choice == '4':  # delete all the table structures and their data
            table_name_list = list(schemaObj.get_table_name_list())
            for i in range(len(table_name_list)):
                table_name = table_name_list[i]
                table_name.strip()
                if table_name:
                    stObj = storage_db.Storage(table_name)
                    stObj.delete_table_data(table_name.strip())  # delete table data
                    del stObj
            schemaObj.deleteAll()  # delete schema from schema file
            choice = input(PROMPT_STR)

        elif choice == '5':  # delete a line of data from the storage file given the keyword
            table_name = input('please input the name of the table to be deleted from:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            field_input = input('please input the field name and the corresponding keyword (fieldname:keyword):')
            if ':' in field_input:
                field_name, keyword = field_input.split(':', 1)
                dataObj = storage_db.Storage(table_name)
                dataObj.delete_record_by_field(field_name.strip(), keyword.strip())
                del dataObj
            else:
                print("Input format error. Please use fieldname:keyword")
            choice = input(PROMPT_STR)

        elif choice == '6':  # update a line of data given the keyword
            table_name = input('please input the name of the table:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            field_name = input('please input the field name:')
            old_value = input('please input the old value of the field:')
            new_value = input('please input the new value of the field:')
            dataObj = storage_db.Storage(table_name)
            dataObj.update_record_by_field(field_name.strip(), old_value.strip(), new_value.strip())
            del dataObj
            choice = input(PROMPT_STR)

        elif choice == '7':  # process SQL statements
            print('#' + '-'*30 + ' SQL QUERY BEGIN ' + '-'*30 + '#')
            sql_str = input('please enter SQL statement: ')
            try:
                lex_db.set_lex_handle()  # to set the global_lexer in common_db.py
                parser_db.set_handle()  # to set the global_parser in common_db.py
                common_db.global_syn_tree = common_db.global_parser.parse(sql_str.strip().lower(), lexer=common_db.global_lexer)  # construct the global_syn_tree
                if common_db.global_syn_tree:
                    query_plan_db.execute_sql_statement(schemaObj) 
                else:
                    print("Failed to parse SQL statement!")
            except Exception as e:
                print(f"Error executing SQL: {e}")
            print('#' + '-'*30 + ' SQL QUERY END ' + '-'*31 + '#')
            choice = input(PROMPT_STR)

        elif choice == '9':  # 索引管理
            index_menu = '''
            1: Create index
            2: Drop index
            3: Test index performance
            4: Back to main menu
            Your choice: '''
            
            while True:
                index_choice = input(index_menu)
                if index_choice == '1':
                    # 创建索引
                    table_name = input('Enter table name: ')
                    if isinstance(table_name, str):
                        table_name = table_name.encode('utf-8')
                    field_name = input('Enter field name to create index: ')
                    index_type = input('Choose index type (1: B-tree, 2: Hash): ')
                    
                    if table_name.strip() in schemaObj.get_table_name_list():
                        from index_db import Index
                        idx = Index(table_name.decode('utf-8'))
                        idx.create_index(field_name)
                        print(f"Index created on {table_name.decode('utf-8')}.{field_name}")
                    else:
                        print("Table not found!")

                elif index_choice == '2':
                    # 删除索引
                    table_name = input('Enter table name: ')
                    if isinstance(table_name, str):
                        table_name = table_name.encode('utf-8')
                    if table_name.strip() in schemaObj.get_table_name_list():
                        import os
                        if os.path.exists(f"{table_name.decode('utf-8')}.ind"):
                            os.remove(f"{table_name.decode('utf-8')}.ind")
                            print("Index dropped successfully")
                        else:
                            print("No index exists for this table")
                            
                elif index_choice == '3':
                    # 测试索引性能
                    table_name = input('Enter table name: ')
                    if isinstance(table_name, str):
                        table_name = table_name.encode('utf-8')
                    field_name = input('Enter field name: ')
                    search_value = input('Enter search value: ')
                    
                    import time
                    # 不使用索引的查询时间
                    start_time = time.time()
                    dataObj = storage_db.Storage(table_name)
                    dataObj.find_record_by_field(field_name, search_value)
                    no_index_time = time.time() - start_time
                    
                    # 使用索引的查询时间
                    from index_db import Index
                    start_time = time.time()
                    idx = Index(table_name.decode('utf-8'))
                    # 假设索引已经建立
                    if os.path.exists(f"{table_name.decode('utf-8')}.ind"):
                        idx.search_by_index(field_name, search_value)
                        with_index_time = time.time() - start_time
                        
                        print(f"\nPerformance comparison:")
                        print(f"Without index: {no_index_time:.4f} seconds")
                        print(f"With index: {with_index_time:.4f} seconds")
                        print(f"Speed improvement: {(no_index_time/with_index_time):.2f}x")
                    else:
                        print("No index exists for this table")
                    
                elif index_choice == '4':
                    break
                    
                else:
                    print("Invalid choice!")
            
            choice = input(PROMPT_STR)

        elif choice == '.':  # quit the program
            print('main loop finishies')
            del schemaObj
            break

        else:
            print('Wrong input, please try again!')
            choice = input(PROMPT_STR)
        
    print('main loop finish!')

if __name__ == '__main__':
    main()