# -----------------------------------------------------------------------
# storage_db.py
# Author: Jingyu Han  hjymail@163.com
# modified by: Xinjian Zhang   278254081@qq.com
# -----------------------------------------------------------------------
# the module is to store tables in files
# Each table is stored in a separate file with the suffix ".dat".
# For example, the table named moviestar is stored in file moviestar.dat 
# -----------------------------------------------------------------------
# struct of file is as follows, each block is 4096
# ---------------------------------------------------
# block_0|block_1|...|block_n
# ----------------------------------------------------------------
import datetime
from common_db import BLOCK_SIZE
# structure of block_0, which stores the meta information and field information
# ---------------------------------------------------------------------------------
# block_id                                # 0
# number_of_dat_blocks                    # at first it is 0 because there is no data in the table
# number_of_fields or number_of_records   # the total number of fields for the table
# -----------------------------------------------------------------------------------------
# the data type is as follows
# ----------------------------------------------------------
# 0->str,1->varstr,2->int,3->bool
# ---------------------------------------------------------------
# structure of data block, whose block id begins with 1
# ----------------------------------------
# block_id       
# number of records
# record_0_offset         # it is a pointer to the data of record
# record_1_offset
# ...
# record_n_offset
# ....
# free space
# ...
# record_n
# ...
# record_1
# record_0
# -------------------------------------------
# structre of one record
# -----------------------------
# pointer                     #offset of table schema in block id 0
# length of record            # including record head and record content
# time stamp of last update  # for example,1999-08-22
# field_0_value
# field_1_value
# ...
# field_n_value
# -------------------------
import struct
import os
import ctypes
# --------------------------------------------
# the class can store table data into files
# functions include insert, delete and update
# --------------------------------------------

class Storage(object):
    # ------------------------------
    # constructor of the class
    # input:
    #       tablename
    # 修改了输出顺序,美化
    # -------------------------------------
    def __init__(self, tablename):
        # print "__init__ of ",Storage.__name__,"begins to execute"
        tablename.strip()
        self.record_list = []
        self.record_Position = []
        if isinstance(tablename,str):
            tablename=tablename.encode('utf-8')
        if not os.path.exists(tablename + '.dat'.encode('utf-8')):  # the file corresponding to the table does not exist
            print(('table file ' + tablename.decode('utf-8') + '.dat does not exists'))
            self.f_handle = open(tablename + '.dat'.encode('utf-8'), 'wb+')
            self.f_handle.close()
            self.open = False
            print(('table file ' + tablename.decode('utf-8') + '.dat has been created'))
        self.f_handle = open(tablename + '.dat'.encode('utf-8'), 'rb+')
        print(f'table file {tablename.decode("utf-8")}.dat has been opened')
        self.open = True
        self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        self.f_handle.seek(0)
        self.dir_buf = self.f_handle.read(BLOCK_SIZE)
        self.dir_buf.strip()
        my_len = len(self.dir_buf)
        self.field_name_list = []
        beginIndex = 0
        if my_len == 0:  # there is no data in the block 0, we should write meta data into the block 0
            if isinstance(tablename, bytes):
                self.num_of_fields = input(
                    "please input the number of feilds in table " + tablename.decode('utf-8') + ":")
            else:
                self.num_of_fields = input(
                    "please input the number of feilds in table " + tablename.decode('utf-8') + ":")
            if int(self.num_of_fields) > 0:
                self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
                self.block_id = 0
                self.data_block_num = 0
                struct.pack_into('!iii', self.dir_buf, beginIndex, 0, 0,int(self.num_of_fields))  # block_id,number_of_data_blocks,number_of_fields
                beginIndex = beginIndex + struct.calcsize('!iii')
                # the following is to write the field name,field type and field length into the buffer in turn
                for i in range(int(self.num_of_fields)):
                    field_name = input("please input the name of field " + str(i+1) + " :")
                    if len(field_name) < 10:
                        field_name = ' ' * (10 - len(field_name.strip())) + field_name
                    while True:
                        field_type = input(
                            "please input the type of field(0-> str; 1-> varstr; 2-> int; 3-> boolean) " + str(i+1) + " :")
                        if int(field_type) in [0, 1, 2, 3]:
                            break
                    # to need further modification here
                    field_length = input("please input the length of field " + str(i+1) + " :")
                    temp_tuple = (field_name, int(field_type), int(field_length))
                    self.field_name_list.append(temp_tuple)
                    if isinstance(field_name, str):
                        field_name = field_name.encode('utf-8')
                    struct.pack_into('!10sii', self.dir_buf, beginIndex, field_name, int(field_type),int(field_length))
                    beginIndex = beginIndex + struct.calcsize('!10sii')
                self.f_handle.seek(0)
                self.f_handle.write(self.dir_buf)
                self.f_handle.flush()
        else:  # there is something in the file
            self.block_id, self.data_block_num, self.num_of_fields = struct.unpack_from('!iii', self.dir_buf, 0)
            print('number of fields is ', self.num_of_fields)
            print('data_block_num', self.data_block_num)
            beginIndex = struct.calcsize('!iii')
            # the followins is to read field name, field type and field length into main memory structures
            for i in range(self.num_of_fields):
                field_name, field_type, field_length = struct.unpack_from('!10sii', self.dir_buf,beginIndex + i * struct.calcsize('!10sii'))  # i means no memory alignment
                temp_tuple = (field_name, field_type, field_length)
                self.field_name_list.append(temp_tuple)
                print(f"the {i+1}th field information (field name, field type, field length) is "
          f"('{field_name.decode('utf-8').strip()}', {field_type}, {field_length})")
        # print self.field_name_list
        record_head_len = struct.calcsize('!ii10s')
        record_content_len = sum(map(lambda x: x[2], self.field_name_list))
        # print record_content_len
        Flag = 1
        while Flag <= self.data_block_num:
            self.f_handle.seek(BLOCK_SIZE * Flag)
            self.active_data_buf = self.f_handle.read(BLOCK_SIZE)
            if len(self.active_data_buf) < 8:
        # 数据块为空或不完整，跳过
                print(f"Warning: Data block {Flag} is empty or incomplete, skipping.")
                Flag += 1
                continue
            self.block_id, self.Number_of_Records = struct.unpack_from('!ii', self.active_data_buf, 0)
            print('Block_ID=%s,   Contains %s data' % (self.block_id, self.Number_of_Records))
            # There exists record
            if self.Number_of_Records > 0:
                for i in range(self.Number_of_Records):
                    self.record_Position.append((Flag, i))
                    offset = \
                        struct.unpack_from('!i', self.active_data_buf,struct.calcsize('!ii') + i * struct.calcsize('!i'))[0]
                    record = struct.unpack_from('!' + str(record_content_len) + 's', self.active_data_buf,offset + record_head_len)[0]
                    tmp = 0
                    tmpList = []
                    for field in self.field_name_list:
                        t = record[tmp:tmp + field[2]].strip()
                        tmp = tmp + field[2]
                        if field[1] == 2:
                            t = int(t)
                        if field[1] == 3:
                            t = bool(t)
                        tmpList.append(t)
                    self.record_list.append(tuple(tmpList))
            Flag += 1

    # ------------------------------
    # return the record list of the table
    # input:
    #       
    # -------------------------------------
    def getRecord(self):
        return self.record_list

    # --------------------------------
    # to insert a record into table
    # param insert_record: list
    # return: True or False
    # -------------------------------
    def insert_record(self, insert_record):
        # example: ['xuyidan','23','123456']
        # step 1 : to check the insert_record is True or False
        tmpRecord = []
        for idx in range(len(self.field_name_list)):
            insert_record[idx] = insert_record[idx].strip()
            if self.field_name_list[idx][1] == 0 or self.field_name_list[idx][1] == 1:
                if len(insert_record[idx]) > self.field_name_list[idx][2]:
                    return False
                tmpRecord.append(insert_record[idx])
            if self.field_name_list[idx][1] == 2:
                try:
                    tmpRecord.append(int(insert_record[idx]))
                except:
                    return False
            if self.field_name_list[idx][1] == 3:
                try:
                    tmpRecord.append(bool(insert_record[idx]))
                except:
                    return False
            insert_record[idx] = ' ' * (self.field_name_list[idx][2] - len(insert_record[idx])) + insert_record[idx]
        # step2: Add tmpRecord to record_list ; change insert_record into inputstr
        inputstr = ''.join(insert_record)
        self.record_list.append(tuple(tmpRecord))
        # Step3: To calculate MaxNum in each Data Blocks
        record_content_len = len(inputstr)
        record_head_len = struct.calcsize('!ii10s')
        record_len = record_head_len + record_content_len
        MAX_RECORD_NUM = (BLOCK_SIZE - struct.calcsize('!i') - struct.calcsize('!ii')) / (record_len + struct.calcsize('!i'))
        # Step4: To calculate new record Position
        if not len(self.record_Position):
            self.data_block_num += 1
            self.record_Position.append((1, 0))
        else:
            last_Position = self.record_Position[-1]
            if last_Position[1] == MAX_RECORD_NUM - 1:
                self.record_Position.append((last_Position[0] + 1, 0))
                self.data_block_num += 1
            else:
                self.record_Position.append((last_Position[0], last_Position[1] + 1))
        last_Position = self.record_Position[-1]
        # Step5: Write new record into file xxx.dat
        # update data_block_num
        self.f_handle.seek(0)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, 0, self.data_block_num)
        self.f_handle.write(self.buf)
        self.f_handle.flush()
        # update data block head
        self.f_handle.seek(BLOCK_SIZE * last_Position[0])
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, last_Position[0], last_Position[1] + 1)
        self.f_handle.write(self.buf)
        self.f_handle.flush()
        # update data offset
        offset = struct.calcsize('!ii') + last_Position[1] * struct.calcsize('!i')
        beginIndex = BLOCK_SIZE - (last_Position[1] + 1) * record_len
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + offset)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!i'))
        struct.pack_into('!i', self.buf, 0, beginIndex)
        self.f_handle.write(self.buf)
        self.f_handle.flush()
        # update data
        record_schema_address = struct.calcsize('!iii')
        update_time = datetime.datetime.now().strftime('%Y-%m-%d')  # update time
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + beginIndex)
        self.buf = ctypes.create_string_buffer(record_len)
        struct.pack_into('!ii10s', self.buf, 0, record_schema_address, record_content_len, update_time.encode('utf-8'))
        struct.pack_into('!' + str(record_content_len) + 's', self.buf, record_head_len, inputstr.encode('utf-8'))
        self.f_handle.write(self.buf.raw)
        self.f_handle.flush()
        return True

    # ------------------------------
    # show the data structure and its data
    # -------------------------------------
    def show_table_data(self):
        print('|'.join(map(lambda x: x[0].decode('utf-8').strip(), self.field_name_list)))  # show the structure
        # the following is to show the data of the table
        for record in self.record_list:
            display = [field.decode('utf-8').strip() if isinstance(field, bytes) else str(field) for field in record]
            print('|'.join(display))

    # --------------------------------
    # to delete  the data file
    # input
    #       table name
    # output
    #       True or False
    # -----------------------------------
    def delete_table_data(self, tableName):
        # step 1: identify whether the file is still open
        if self.open == True:
            self.f_handle.close()
            self.open = False
        # step 2: remove the file from os   
        tableName.strip()
        if os.path.exists(tableName + '.dat'.encode('utf-8')):
            os.remove(tableName + '.dat'.encode('utf-8'))
        return True

    # ------------------------------
    # get the list of field information, each element of which is (field name, field type, field length)
    # input:
    #       
    # -------------------------------------
    def getFieldList(self):
        return self.field_name_list

    # ----------------------------------------
    # destructor
    # ------------------------------------------------
    def __del__(self):  # write the metahead information in head object to file
        if self.open == True:
            self.f_handle.seek(0)
            self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
            struct.pack_into('!ii', self.buf, 0, 0, self.data_block_num)
            self.f_handle.write(self.buf)
            self.f_handle.flush()
            self.f_handle.close()
    
    # ----------------------------------------------
    # Author: Xinjian Zhang
    # to delete the first record matching a field value and rewrite the data file
    # input
    #       field_name: the field to match
    #       keyword: the value to match
    # output
    #       True or False
    # ------------------------------------------------
    def delete_record_by_field(self, field_name, keyword):
        field_index = None
        for idx, field in enumerate(self.field_name_list):
            name = field[0].decode('utf-8').strip() if isinstance(field[0], bytes) else str(field[0]).strip()
            if name == field_name:
                field_index = idx
                break
        if field_index is None:
            print(f"Field '{field_name}' not found.")
            return False
        new_records = []
        deleted = False
        for record in self.record_list:
            value = record[field_index]
            value_str = value.decode('utf-8').strip() if isinstance(value, bytes) else str(value).strip()
            if not deleted and value_str == keyword:
                deleted = True
                continue
            new_records.append(record)
        if deleted:
            self.record_list = new_records
            self._rewrite_data_file()
            print("Record deleted.")
            return True
        else:
            print("No matching record found.")
            return False

    # ----------------------------------------------
    # Author: Xinjian Zhang
    # to update the first record matching a field value and rewrite the data file
    # input
    #       field_name: the field to match
    #       old_value: the value to be replaced
    #       new_value: the new value
    # output
    #       True or False
    # ------------------------------------------------
    def update_record_by_field(self, field_name, old_value, new_value):
        field_index = None
        for idx, field in enumerate(self.field_name_list):
            name = field[0].decode('utf-8').strip() if isinstance(field[0], bytes) else str(field[0]).strip()
            if name == field_name:
                field_index = idx
                break
        if field_index is None:
            print(f"Field '{field_name}' not found.")
            return False
        updated = False
        for i, record in enumerate(self.record_list):
            value = record[field_index]
            value_str = value.decode('utf-8').strip() if isinstance(value, bytes) else str(value).strip()
            if value_str == old_value:
                record = list(record)
                if isinstance(record[field_index], int):
                    try:
                        record[field_index] = int(new_value)
                    except:
                        print("Invalid int value.")
                        return False
                elif isinstance(record[field_index], bool):
                    record[field_index] = bool(new_value)
                else:
                    record[field_index] = new_value
                self.record_list[i] = tuple(record)
                updated = True
                break
        if updated:
            self._rewrite_data_file()
            print("Record updated.")
            return True
        else:
            print("No matching record found.")
            return False

    # ----------------------------------------------
    # Author: Xinjian Zhang
    # to rewrite the entire data file after delete or update
    # input
    #       None (uses self.record_list)
    # output
    #       None
    # ------------------------------------------------
    def _rewrite_data_file(self):
        # 重新写入 block0 的字段定义
        self.f_handle.seek(0)
        self.f_handle.truncate(0)
        dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        beginIndex = 0
        struct.pack_into('!iii', dir_buf, beginIndex, 0, 0, len(self.field_name_list))
        beginIndex += struct.calcsize('!iii')
        for field in self.field_name_list:
            field_name = field[0]
            if isinstance(field_name, str):
                field_name = field_name.encode('utf-8')
            struct.pack_into('!10sii', dir_buf, beginIndex, field_name, field[1], field[2])
            beginIndex += struct.calcsize('!10sii')
        self.f_handle.seek(0)
        self.f_handle.write(dir_buf)
        self.f_handle.flush()
        # 重新写入数据块
        if self.record_list:
            data_block_num = 1
            num_records = len(self.record_list)
            record_head_len = struct.calcsize('!ii10s')
            record_content_len = sum(map(lambda x: x[2], self.field_name_list))
            record_len = record_head_len + record_content_len
            MAX_RECORD_NUM = int((BLOCK_SIZE - struct.calcsize('!i') - struct.calcsize('!ii')) / (record_len + struct.calcsize('!i')))
            # 只写一个数据块，简化实现
            data_buf = ctypes.create_string_buffer(BLOCK_SIZE)
            struct.pack_into('!ii', data_buf, 0, data_block_num, num_records)
            for i, record in enumerate(self.record_list):
                offset = struct.calcsize('!ii') + i * struct.calcsize('!i')
                beginIndex = BLOCK_SIZE - (i + 1) * record_len
                struct.pack_into('!i', data_buf, offset, beginIndex)
                record_schema_address = struct.calcsize('!iii')
                update_time = datetime.datetime.now().strftime('%Y-%m-%d')
                struct.pack_into('!ii10s', data_buf, beginIndex, record_schema_address, record_content_len, update_time.encode('utf-8'))
                # 拼接字段内容
                inputstr = b''
                for idx, field in enumerate(self.field_name_list):
                    val = record[idx]
                    if isinstance(val, int):
                        val = str(val).encode('utf-8')
                    elif isinstance(val, str):
                        val = val.encode('utf-8')
                    elif isinstance(val, bytes):
                        val = val
                    else:
                        val = str(val).encode('utf-8')
                    val = b' ' * (field[2] - len(val)) + val if len(val) < field[2] else val[:field[2]]
                    inputstr += val
                struct.pack_into('!' + str(record_content_len) + 's', data_buf, beginIndex + record_head_len, inputstr)
            self.f_handle.seek(BLOCK_SIZE)
            self.f_handle.write(data_buf)
            self.f_handle.flush()
            # 更新 block0 的 data_block_num
            self.f_handle.seek(0)
            self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
            struct.pack_into('!ii', self.buf, 0, 0, 1)
            self.f_handle.write(self.buf)
            self.f_handle.flush()
        else:
            # 没有数据时，data_block_num 必须为0，且block0结构完整
            self.f_handle.seek(0)
            dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
            beginIndex = 0
            struct.pack_into('!iii', dir_buf, beginIndex, 0, 0, len(self.field_name_list))
            beginIndex += struct.calcsize('!iii')
            for field in self.field_name_list:
                field_name = field[0]
                if isinstance(field_name, str):
                    field_name = field_name.encode('utf-8')
                struct.pack_into('!10sii', dir_buf, beginIndex, field_name, field[1], field[2])
                beginIndex += struct.calcsize('!10sii')
            self.f_handle.write(dir_buf)
            self.f_handle.flush()

    def getfilenamelist(self):
        # 返回字段信息列表，每个元素是 (字段名, 字段类型, 字段长度)
        return self.field_name_list