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
 | 5: SELECT FROM WHERE clause             |
 | 6: Delete a row by field keyword        |
 | 7: Update a row by field keyword   bb     |
 | .: Quit                                 |
 +-----------------------------------------+
 Input your choice: '''  # the prompt string for user input(美化了选择窗口)

# --------------------------
# the main loop, which needs further implementation
# ---------------------------
def main():
    # main loops for the whole program
    print('main function begins to execute')
    # The instance data of table is stored in binary format, which corresponds to chapter 2-8 of textbook
    schemaObj = schema_db.Schema(debug=False)  # to create a schema object, which contains the schema of all tables(增加了调试选项)
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
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            if not schemaObj.find_table(table_name.strip()):
                print(f"Table {table_name.decode('utf-8') if isinstance(table_name, bytes) else table_name.strip()} does not exist!")
                choice = input(PROMPT_STR)
                continue
            # ...后续删除逻辑...
            choice = input(PROMPT_STR)

        elif choice == '3':  # view the table structure and all the data
            print("Current tables:")
            for t in schemaObj.headObj.tableNames:
                if isinstance(t[0], bytes):
                    print(t[0].decode('utf-8').strip())
                else:
                    print(str(t[0]).strip())
            table_name = input('please input the name of the table to be displayed:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            if not schemaObj.find_table(table_name.strip()):
                print(f"Table {table_name.decode('utf-8') if isinstance(table_name, bytes) else table_name.strip()} does not exist!")
                choice = input(PROMPT_STR)
                continue
            dataObj = storage_db.Storage(table_name)
            dataObj.show_table_data()
            del dataObj
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

        elif choice == '5':  # process SELECT FROM WHERE clause
            sql_str = input('please enter the select from where clause:')
            # 解析SQL，提取from_list
            lex_db.set_lex_handle()
            parser_db.set_handle()
            common_db.global_syn_tree = common_db.global_parser.parse(sql_str.strip(),lexer=common_db.global_lexer)
            from_list = query_plan_db.extract_sfw_data()[1]
            # 检查所有表是否存在
            all_exist = True
            for table_name in from_list:
                if isinstance(table_name, str):
                    table_name = table_name.encode('utf-8')
                if not schemaObj.find_table(table_name.strip()):
                    print(f"Table {table_name.decode('utf-8') if isinstance(table_name, bytes) else table_name.strip()} does not exist!")
                    all_exist = False
            if not all_exist:
                choice = input(PROMPT_STR)
                continue
            choice = input(PROMPT_STR)

        elif choice == '6':  # delete a line of data from the storage file given the keyword
            table_name = input('please input the name of the table to be deleted from:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            if not schemaObj.find_table(table_name.strip()):
                print(f"Table {table_name.decode('utf-8') if isinstance(table_name, bytes) else table_name.strip()} does not exist!")
                choice = input(PROMPT_STR)
                continue
            field_input = input('please input the field name and the corresponding keyword (fieldname:keyword):')
            if ':' in field_input:
                field_name, keyword = field_input.split(':', 1)
                dataObj = storage_db.Storage(table_name)
                dataObj.delete_record_by_field(field_name.strip(), keyword.strip())
                del dataObj
            else:
                print("Input format error. Please use fieldname:keyword")
            choice = input(PROMPT_STR)

        elif choice == '7':  # update a line of data given the keyword
            table_name = input('please input the name of the table:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            if not schemaObj.find_table(table_name.strip()):
                print(f"Table {table_name.decode('utf-8') if isinstance(table_name, bytes) else table_name.strip()} does not exist!")
                choice = input(PROMPT_STR)
                continue
            field_name = input('please input the field name:')
            old_value = input('please input the old value of the field:')
            new_value = input('please input the new value of the field:')
            dataObj = storage_db.Storage(table_name)
            dataObj.update_record_by_field(field_name.strip(), old_value.strip(), new_value.strip())
            del dataObj
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