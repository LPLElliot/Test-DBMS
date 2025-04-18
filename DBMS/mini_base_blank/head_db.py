#---------------------------------
# head_db.py
# author: Jingyu Han    hjymail@163.com
# modified by: Xinjian Zhang   278254081@qq.com
#--------------------------------------
# The main memory structure of table schema
#------------------------------------
class Header(object): 
    #------------------------
    # Constructor of the class
    # Input:
    #   nameList    : Table name list, where each element is a tuple (table_name, num_of_fields, offset_in_body).
    #   fieldDict   : Field dictionary for all tables, where each element is (table_name, fieldList).
    #                 fieldList is a list of fields, and each field is a tuple (field_name, field_type, field_length).
    #   inistored   : Boolean indicating whether the schema is stored.
    #   inLen       : Number of tables.
    #   off         : Offset where the free space begins in the body of the schema file.
    #---------------------------------------------------------------
    def __init__(self, nameList, fieldDict, inistored, inLen, off):
        'Constructor of Header'
        print('__init__ of Header')
        self.isStored = inistored  # Whether the schema is stored
        self.lenOfTableNum = inLen  # Number of tables
        self.offsetOfBody = off  # Offset of the body section
        self.tableNames = nameList  # List of table names
        self.tableFields = fieldDict  # Dictionary of table fields
        self.cache = {}  # Cache for table schemas(modefied later)
        print("isStored is", self.isStored, "tableNum is", self.lenOfTableNum, "offset is", self.offsetOfBody)

    #-----------------------------
    # Destructor of the class
    #-----------------------------
    def __del__(self):
        print('del Header')

    #----------------------------------------------------------
    # Display the schema of all the tables in the schema file
    #----------------------------------------------------------
    def showTables(self):#(modified later)
        if self.lenOfTableNum > 0:
            print("The length of tableNames is", len(self.tableNames))
            for i in range(len(self.tableNames)):
                print("Table Name:", self.tableNames[i])
                print("Fields:", self.tableFields.get(self.tableNames[i][0].strip(), []))
        else:
            print("No tables found in the schema.")

    #----------------------------------------------------------
    # Cache a table schema
    # Input:
    #   table_name: The name of the table to cache.
    #   fields: The fields of the table to cache.
    #----------------------------------------------------------
    def cache_table(self, table_name, fields):#(modified later)
        self.cache[table_name.strip()] = fields
        print(f"Table {table_name.strip()} has been cached.")

    #-----------------------------------------------------------------------
    # Retrieve a cached table schema
    # Input:
    #   table_name: The name of the table to retrieve from the cache.
    # Output:
    #   The cached schema of the table, or None if the table is not cached.
    #-----------------------------------------------------------------------
    def get_cached_table(self, table_name):#(modified later)
        cached_table = self.cache.get(table_name.strip(), None)
        if cached_table:
            print(f"Retrieved cached schema for table {table_name.strip()}.")
        else:
            print(f"No cached schema found for table {table_name.strip()}.")
        return cached_table