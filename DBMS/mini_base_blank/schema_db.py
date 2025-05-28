#-----------------------------------------------
# schema_db.py
# author: Jingyu Han   hjymail@163.com
# modified by: Xinjian Zhang   278254081@qq.com
#-----------------------------------------------
# to process the schema data, which is stored in all.sch
# all.sch are divied into three parts,namely metaHead, tableNameHead and body
# metaHead|tableNameHead|body
#-------------------------------------------

import ctypes
import struct
import head_db # it is main memory structure for the table schema

#the following is metaHead structure,which is 12 bytes
"""
isStored    # whether there is data in the all.sch
tableNum    # how many tables
offset      # where the free area begins for body.
"""
META_HEAD_SIZE=12                                           #the First part in the schema file

#the following is the structure of tableNameHead
"""
tablename|numofFeilds|beginOffsetInBody|....|tablename|numofFeilds|beginOffsetInBody|
10 bytes |4 bytes    |4 bytes
"""
MAX_TABLE_NAME_LEN=10                                       # the maximum length of table name
MAX_TABLE_NUM=100                                           # the maximum number of tables in the all.sch
TABLE_NAME_ENTRY_LEN=MAX_TABLE_NAME_LEN+4+4                 # the length of one table name entry
TABLE_NAME_HEAD_SIZE=MAX_TABLE_NUM*TABLE_NAME_ENTRY_LEN     # the SECOND part in the schema file

# the following is for body, which stores the field information of each table and the field information is as follows
"""
field_name   # it is a string
field_type   # it is an integer, 0->str,1->varstr,2->int,3->bool
field_length # it is an integer
"""
MAX_FIELD_NAME_LEN=10                                       # the maximum length of field name
MAX_FIELD_LEN=10+4+4                                         #  the maximum length of one field
MAX_NUM_OF_FIELD_PER_TABLE=5                                # the maximum number of fields in one table
FIELD_ENTRY_SIZE_PER_TABLE=MAX_FIELD_LEN*MAX_NUM_OF_FIELD_PER_TABLE
MAX_FIELD_SECTION_SIZE=FIELD_ENTRY_SIZE_PER_TABLE*MAX_TABLE_NUM #the THIRD part in the schema file
BODY_BEGIN_INDEX=META_HEAD_SIZE+TABLE_NAME_HEAD_SIZE            # Intitially, where the field name, type and length are stored

# -----------------------------
# the table name is padded if its lenght is smaller than MAX_TABLE_NAME_WHEN
# input:
#       tableName: the table name       
# -------------------------------
def fillTableName(tableName): # it should be 10 bytes
    if len(tableName.strip())<MAX_TABLE_NAME_LEN:
        tableName=(' '*(MAX_TABLE_NAME_LEN-len(tableName.strip()))).encode('utf-8')+tableName.strip()
        return tableName

class Schema(object):
    fileName = 'DBMS/mini_base_blank/all.sch'  # the schema file name
    count = 0  # there should be only one object in the program
    @staticmethod

    def how_many():  # give the count of instances
        return Schema.count

    def viewTableNames(self):  # to list all the table names in the all.sch
        print ('viewtablenames begin to execute')
        for i in self.headObj.tableNames:
            print ('Table name is ', i[0])
        print ('execute Done!')

    # ------------------------------------------------
    # constructor of the class
    # 增加了调试语句,优化了输出
    # ------------------------------------------------
    def __init__(self,debug=False):
        self.debug = debug
        if self.debug:
            print('__init__ of Schema')
            print('schema fileName is ' + Schema.fileName)
        self.fileObj = open(Schema.fileName, 'rb+')  # in binary format
        # read all data from schema file
        bufLen = META_HEAD_SIZE + TABLE_NAME_HEAD_SIZE + MAX_FIELD_SECTION_SIZE  # the length of metahead, table name entries and feildName sections
        buf = ctypes.create_string_buffer(bufLen)
        buf = self.fileObj.read(bufLen)
        #the following is to print the content of the buffer
        buf.strip()
        if len(buf) == 0:  # for the first time, there is nothing in the schema file
            self.body_begin_index = BODY_BEGIN_INDEX
            buf = struct.pack('!?ii', False, 0, self.body_begin_index)  # is_stored, tablenum,offset
            self.fileObj.seek(0)
            self.fileObj.write(buf)
            self.fileObj.flush()
            # the following is to create a main memory structure for the schema=
            nameList = []
            fieldsList = {}
            self.headObj = head_db.Header(nameList, fieldsList,False, 0, self.body_begin_index)
            if self.debug:
                print('metaHead of schema has been written to all.sch and the Header object created')
        else:  # there is something in the schema file
            if self.debug:
                print ("there is something  in the all.sch")
            # in the following ? denotes bool type and  i denotes int type
            isStored, tempTableNum, tempOffset = struct.unpack_from('!?ii', buf, 0)   #link:https://docs.python.org/2/library/struct.html
            Schema.body_begin_index = tempOffset
            nameList=[]
            fieldsList={}
            if not isStored:
                self.headObj = head_db.Header(nameList, fieldsList, False, 0, BODY_BEGIN_INDEX)
                if self.debug:
                    print('there is no table in the file')
                else:
                    print("Schema loaded. No tables found.")
            else:
                if self.debug:
                    print(f'tableNum in schema file is {tempTableNum}')
                    print(f'isStored in schema file is {isStored}')
                    print(f'offset of body in schema file is {tempOffset}')
                print(f"Schema loaded. Table count: {tempTableNum}")
                for i in range(tempTableNum):
                    # fetch the table name in tableNameHead
                    tempName, = struct.unpack_from('!10s', buf,META_HEAD_SIZE + i * TABLE_NAME_ENTRY_LEN)  # Note: '!' means no memory alignment
                    if self.debug:
                        print("--------------------")
                        print(f"table {i+1} is {tempName.decode('utf-8').strip()}")
                    # fetch the number of fields in the table in tableNameHead
                    tempNum, = struct.unpack_from('!i', buf, META_HEAD_SIZE + i * TABLE_NAME_ENTRY_LEN + 10)
                    if self.debug:
                        print(f"number of fields of table {tempName.decode('utf-8').strip()} is {tempNum}")
                    # fetch the offset where field names are stored in the body
                    tempPos, = struct.unpack_from('!i', buf,META_HEAD_SIZE + i * TABLE_NAME_ENTRY_LEN + 10 + struct.calcsize('i'))
                    if self.debug:
                        print(f"tempPos in body is {tempPos}")
                    tempNameMix = (tempName.strip(), tempNum, tempPos)
                    nameList.append(tempNameMix)  # It is a triple
                    # the following is to fetch field information from body section and each field is  (fieldname,fieldtype,fieldlength)
                    if tempNum > 0: # the number of fields is greater than 0
                        fields = []  # it is a list
                        for j in range(tempNum):
                            tempFieldName,tempFieldType,tempFieldLength = struct.unpack_from('!10sii',buf, tempPos + j * MAX_FIELD_LEN)
                            if self.debug:
                                print (f"field name is {tempFieldName.decode('utf-8').strip()}")
                                print ('field type is', tempFieldType)
                                print ('filed length is', tempFieldLength)
                            tempFieldTuple=(tempFieldName,tempFieldType,tempFieldLength)
                            fields.append(tempFieldTuple)
                        if self.debug:
                            print("---------------------")
                        fieldsList[tempName.strip()]=fields
                # the main memory structure for schema is constructed
                self.headObj = head_db.Header(nameList, fieldsList, True, tempTableNum, tempOffset)

    # ----------------------------
    # destructor of the class
    # ----------------------------
    def __del__(self):  # write the metahead information in head object to file
        print ("__del__ of class Schema begins to execute")
        buf = ctypes.create_string_buffer(12)
        struct.pack_into('!?ii', buf, 0, self.headObj.isStored, self.headObj.lenOfTableNum, self.headObj.offsetOfBody)
        self.fileObj.seek(0)
        self.fileObj.write(buf)
        self.fileObj.flush()
        self.fileObj.close()

    # --------------------------
    # delete all the contents in the schema file
    # ----------------------------------------
    def deleteAll(self):
        self.headObj.tableFields={}
        self.headObj.tableNames=[]
        self.fileObj.seek(0)
        self.fileObj.truncate(0)
        self.headObj.isStored = False
        self.headObj.lenOfTableNum = 0
        self.headObj.offsetOfBody = self.body_begin_index
        self.fileObj.flush()
        print ("all.sch file has been truncated")

    # -----------------------------
    # insert a table schema to the schema file
    # input:
    #       tablename: the table to be added
    #       fieldList: the field information list and each element is a tuple(fieldname,fieldtype,fieldlength)
    # -------------------------------
    def appendTable(self, tableName, fieldList):  # it modify the tableNameHead and body of all.sch
        print ("appendTable begins to execute")
        tableName.strip()
        if len(tableName) == 0 or len(tableName) > 10 or len(fieldList)==0:
            print ('tablename is invalid or field list is invalid')
        else:
            fieldNum = len(fieldList)
            print ("the following is to write the fields to body in all.sch")
            fieldBuff = ctypes.create_string_buffer(MAX_FIELD_LEN * len(fieldList))
            beginIndex = 0
            for i in range(len(fieldList)):
                fieldName, fieldType, fieldLength = fieldList[i]
                if isinstance(fieldName, bytes):
                    fieldName = fieldName.decode('utf-8')
                fieldName = fieldName.strip()
                filledFieldName = fieldName.ljust(MAX_FIELD_NAME_LEN) 
                struct.pack_into('!10sii', fieldBuff, beginIndex, filledFieldName.encode('utf-8'), int(fieldType), int(fieldLength))
                beginIndex += MAX_FIELD_LEN
            writePos = self.headObj.offsetOfBody
            self.fileObj.seek(writePos)
            self.fileObj.write(fieldBuff)
            self.fileObj.flush()
            # self.headObj.offsetOfBody=self.headObj.offsetBody+fieldNum*MAX_FIELD_LEN
            print ("the following is to write table name entry to tableNameHead in all.sch")
            filledTableName = fillTableName(tableName)
            if isinstance(filledTableName, str):
                filledTableName = filledTableName.encode('utf-8')
            nameBuf = struct.pack('!10sii', filledTableName, fieldNum, self.headObj.offsetOfBody)
            self.fileObj.seek(META_HEAD_SIZE + self.headObj.lenOfTableNum * TABLE_NAME_ENTRY_LEN)
            nameContent = (tableName.strip(), fieldNum, self.headObj.offsetOfBody)
            self.fileObj.write(nameBuf)
            self.fileObj.flush()
            print ("to modify the header structure in main memory")
            self.headObj.isStored = True
            self.headObj.lenOfTableNum += 1
            self.headObj.offsetOfBody += fieldNum * MAX_FIELD_LEN
            self.headObj.tableNames.append(nameContent)
            self.headObj.tableFields[tableName.strip()]=fieldList

    # -------------------------------
    # to determine whether the table named table_name exist, depending on the main memory structures
    # input
    #       table_name
    # output
    #       true or false
    # -------------------------------------------------------
    def find_table(self, table_name):
        Tables = map(lambda x: x[0], self.headObj.tableNames)
        if table_name in Tables:
            return True
        else:
            return False

    # ----------------------------------------------
    # to write the main memory information into the schema file
    # input
    #       
    # output
    #       True or False
    # ------------------------------------------------   
    def WriteBuff(self):
        bufLen = META_HEAD_SIZE + TABLE_NAME_HEAD_SIZE + MAX_FIELD_SECTION_SIZE  # the length of metahead, table name entries and feildName sections
        buf = ctypes.create_string_buffer(bufLen)
        struct.pack_into('!?ii', buf, 0, self.headObj.isStored, self.headObj.lenOfTableNum, self.headObj.offsetOfBody)
        #isStored, tempTableNum, tempOffset = struct.unpack_from('!?ii', buf,0)  # link:https://docs.python.org/2/library/struct.html
        #print isStored,tempTableNum,tempOffset
        for idx in range(len(self.headObj.tableNames)):
            tmp_tableName = self.headObj.tableNames[idx][0]
            if len(tmp_tableName)<10:
                tmp_tableName = b' ' * (10 - len(tmp_tableName.strip())) + tmp_tableName
            # write (tablename,numberoffields,offsetinbody) to buffer
            struct.pack_into('!10sii', buf, META_HEAD_SIZE + idx * TABLE_NAME_ENTRY_LEN, tmp_tableName,
                             self.headObj.tableNames[idx][1],self.headObj.tableNames[idx][2])
            # write the field information of each table into the buffer
            fields = self.headObj.tableFields[tmp_tableName.strip()]
            for idj in range(self.headObj.tableNames[idx][1]):
                (tempFieldName,tempFieldType,tempFieldLength)=fields[idj]             
                struct.pack_into('!10sii',buf,self.headObj.tableNames[idx][2]+idj*MAX_FIELD_LEN,
                                tempFieldName,tempFieldType,tempFieldLength)
        self.fileObj.seek(0)
        self.fileObj.write(buf)
        self.fileObj.flush()

    # ----------------------------------------------
    # Author: Xinjian Zhang
    # to delete the schema of a table from the schema file
    # input
    #       table_name: the table to be deleted
    # output
    #       True or False
    # ------------------------------------------------
    def delete_table_schema(self, table_name):
        tmpIndex = -1
        if isinstance(table_name, str):
            table_name = table_name.encode('utf-8')
        table_name = table_name.strip()
        # 查找要删除的表索引
        for i in range(len(self.headObj.tableNames)):
            tname = self.headObj.tableNames[i][0]
            if isinstance(tname, bytes):
                tname = tname.strip()
            else:
                tname = str(tname).strip().encode('utf-8')
            if tname == table_name:
                tmpIndex = i
                break
        if tmpIndex >= 0:
            # 删除 tableNames
            del self.headObj.tableNames[tmpIndex]
            del self.headObj.tableFields[table_name]
            self.headObj.lenOfTableNum -= 1
            # 更新头部信息
            if len(self.headObj.tableNames) > 0:
                name_list = [x[0] for x in self.headObj.tableNames]
                field_num_per_table = [x[1] for x in self.headObj.tableNames]
                table_offset = [0] * len(self.headObj.tableNames)
                table_offset[0] = BODY_BEGIN_INDEX
                for idx in range(1, len(table_offset)):
                    table_offset[idx] = table_offset[idx-1] + field_num_per_table[idx-1] * MAX_FIELD_LEN
                self.headObj.tableNames = list(zip(name_list, field_num_per_table, table_offset))
                self.headObj.offsetOfBody = self.headObj.tableNames[-1][2] + self.headObj.tableNames[-1][1] * MAX_FIELD_LEN
                self.WriteBuff()
            # 更新文件
            else:
                self.headObj.offsetOfBody = BODY_BEGIN_INDEX
                self.headObj.isStored = False
                self.WriteBuff()
            return True
        else:
            print('Cannot find the table!')
            return False

    # ---------------------------
    # to return the list of all the table names
    # input
    # output
    #       table_name_list: the returned list of table names
    # --------------------------------
    def get_table_name_list(self):
        return map(lambda x:x[0],self.headObj.tableNames)