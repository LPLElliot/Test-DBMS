#------------------------------------------------
# query_plan_db.py
# author: Jingyu Han  hjymail@163.com
# modified by: Xinjian Zhang   278254081@qq.com
#------------------------------------------------
#----------------------------------------------------------
# this module can turn a syntax tree into a query plan tree
#----------------------------------------------------------
import common_db
import storage_db
import schema_db
import itertools 
import uuid
import datetime
import log_db

class parseNode:
    def __init__(self):
        self.sel_list = []
        self.from_list = []
        self.where_list = []
    
    def get_sel_list(self):
        return self.sel_list
    
    def get_from_list(self):
        return self.from_list
    
    def get_where_list(self):
        return self.where_list
    
    def update_sel_list(self, sel_list):
        self.sel_list = sel_list
    
    def update_from_list(self, from_list):
        self.from_list = from_list
    
    def update_where_list(self, where_list):
        self.where_list = where_list

def extract_sfw_data():
    print('extract_sfw_data begins to execute')
    syn_tree = common_db.global_syn_tree  
    if syn_tree is None:
        print('wrong')
        return [], [], []
    else:
        def find_sfw(node):
            if isinstance(node, common_db.Node):
                if node.value == 'SFW':
                    return node
                if node.children:
                    for child in node.children:
                        res = find_sfw(child)
                        if res:
                            return res
            return None
        
        sfw_node = find_sfw(syn_tree)
        if sfw_node is None:
            print('No SFW node found in syntax tree')
            return [], [], []
        
        PN = parseNode()
        destruct(sfw_node, PN)
        return PN.get_sel_list(), PN.get_from_list(), PN.get_where_list()

def destruct(nodeobj, PN):
    if isinstance(nodeobj, common_db.Node):
        if nodeobj.children:
            if nodeobj.value == 'SelList':
                tmpList = []
                show(nodeobj, tmpList)
                PN.update_sel_list(tmpList)
            elif nodeobj.value == 'FromList':
                tmpList = []
                show(nodeobj, tmpList)
                PN.update_from_list(tmpList)
            elif nodeobj.value == 'Cond':
                tmpList = []
                show(nodeobj, tmpList)
                PN.update_where_list(tmpList)
            else:
                for i in range(len(nodeobj.children)):
                    destruct(nodeobj.children[i], PN)

def show(nodeobj, tmpList):
    if isinstance(nodeobj, common_db.Node):
        if not nodeobj.children:
            if isinstance(nodeobj.value, bytes):
                tmpList.append(nodeobj.value.decode('utf-8').strip())
            else:
                tmpList.append(str(nodeobj.value).strip())
        else:
            for i in range(len(nodeobj.children)):
                show(nodeobj.children[i], tmpList)
    elif isinstance(nodeobj, str):
        tmpList.append(nodeobj.strip())

def construct_from_node(from_list):
    if from_list:        
        if len(from_list) == 1:
            temp_node = common_db.Node(from_list[0], None)
            return common_db.Node('X', [temp_node])
        elif len(from_list) == 2:
            temp_node_first = common_db.Node(from_list[0], None)
            temp_node_second = common_db.Node(from_list[1], None)
            return common_db.Node('X', [temp_node_first, temp_node_second])       
        elif len(from_list) > 2:
            right_node = common_db.Node(from_list[len(from_list)-1], None)
            return common_db.Node('X', [construct_from_node(from_list[0:len(from_list)-1]), right_node])

def construct_where_node(from_node, where_list):
    if from_node and len(where_list) > 0:
       return common_db.Node('Filter', [from_node], where_list)
    elif from_node and len(where_list) == 0:
        return from_node

def construct_select_node(wf_node, sel_list):
    if wf_node and len(sel_list) > 0:
        if sel_list[0] == '*' or sel_list[0] == 'STAR':
            return common_db.Node('Proj', [wf_node], ['*'])
        return common_db.Node('Proj', [wf_node], sel_list)

def execute_logical_tree():
    if common_db.global_logical_tree:
        def excute_tree():
            idx = 0
            dict_ = {}
            def show_tree(node_obj, idx, dict_):
                if isinstance(node_obj, common_db.Node):  # it is a Node object
                    dict_.setdefault(idx, [])
                    dict_[idx].append(node_obj.value)
                    if node_obj.var:
                        dict_[idx][-1] = tuple((dict_[idx][-1], node_obj.var))
                    if node_obj.children:
                        for i in range(len(node_obj.children)):
                            show_tree(node_obj.children[i], idx + 1, dict_)
            show_tree(common_db.global_logical_tree, idx, dict_)
            idx = sorted(dict_.keys(), reverse=True)[0]
            def GetFilterParam(tableName_Order, current_field, param):
                if isinstance(param, bytes):
                    param = param.decode('utf-8')
                if '.' in param:
                    tableName = param.split('.')[0]
                    FieldName = param.split('.')[1]
                    if tableName in tableName_Order:
                        TableIndex = tableName_Order.index(tableName)
                    else:
                        return 0, 0, 0, False
                elif len(tableName_Order) == 1:
                    TableIndex = 0
                    FieldName = param
                else:
                    return 0, 0, 0, False
                tmp = [x[0].decode('utf-8').strip() if isinstance(x[0], bytes) else str(x[0]).strip() for x in current_field[TableIndex]]
                if FieldName in tmp:
                    FieldIndex = tmp.index(FieldName)
                    FieldType = current_field[TableIndex][FieldIndex][1]
                    return TableIndex, FieldIndex, FieldType, True
                else:
                    return 0, 0, 0, False
            
            current_field = []
            current_list =[]
            while (idx >= 0):
                if idx == sorted(dict_.keys(), reverse=True)[0]:
                    if len(dict_[idx]) > 1:
                        t1 = dict_[idx][0]
                        t2 = dict_[idx][1]
                        if isinstance(t1, bytes):
                            t1 = t1.decode('utf-8')
                        if isinstance(t2, bytes):
                            t2 = t2.decode('utf-8')
                        a_1 = storage_db.Storage(t1)
                        a_2 = storage_db.Storage(t2)
                        current_list = []
                        tableName_Order = [t1, t2]
                        current_field = [a_1.getfilenamelist(), a_2.getfilenamelist()]
                        for x in itertools.product(a_1.getRecord(), a_2.getRecord()):
                            current_list.append(list(x))
                    else:
                        t1 = dict_[idx][0]
                        if isinstance(t1, bytes):
                            t1 = t1.decode('utf-8')
                        a_1 = storage_db.Storage(t1)
                        current_list = a_1.getRecord()
                        tableName_Order = [t1]
                        current_field = [a_1.getfilenamelist()]
                elif 'X' in dict_[idx] and len(dict_[idx]) > 1:
                    t2 = dict_[idx][1]
                    if isinstance(t2, bytes):
                        t2 = t2.decode('utf-8')
                    a_2 = storage_db.Storage(t2)
                    tableName_Order.append(t2)
                    current_field.append(a_2.getfilenamelist())
                    tmp_List = current_list[:]
                    current_list = []
                    for x in itertools.product(tmp_List, a_2.getRecord()):
                        current_list.append(list((x[0][0], x[0][1], x[1])))
                elif 'X' not in dict_[idx]:
                    if 'Filter' in dict_[idx][0]:
                        FilterChoice = dict_[idx][0][1]
                        TableIndex, FieldIndex, FieldType, isTrue = GetFilterParam(tableName_Order, current_field,FilterChoice[0])
                        if not isTrue:
                            return [], [], False
                        else:
                            if FieldType == 2:
                                FilterParam = int(FilterChoice[2].strip())
                            elif FieldType == 3:
                                FilterParam = bool(FilterChoice[2].strip())
                            else:
                                FilterParam = FilterChoice[2].strip()
                        tmp_List = current_list[:]
                        current_list = []
                        for tmpRecord in tmp_List:
                            if len(current_field) == 1:
                                ans = tmpRecord[FieldIndex]
                            else:
                                ans = tmpRecord[TableIndex][FieldIndex]
                            if FieldType == 0 or FieldType == 1:
                                ans = ans.strip() if isinstance(ans, bytes) else str(ans).strip()
                            if isinstance(FilterParam, bytes):
                                FilterParam = FilterParam.decode('utf-8').strip()
                            else:
                                FilterParam = str(FilterParam).strip()
                            if isinstance(ans, bytes):
                                ans = ans.decode('utf-8').strip()
                            else:
                                ans = str(ans).strip()
                            if FilterParam == ans:
                                current_list.append(tmpRecord)
                    if 'Proj' in dict_[idx][0]:
                        SelIndexList = []
                        if dict_[idx][0][1][0] == '*':
                            for ti, fields in enumerate(current_field):
                                for fi in range(len(fields)):
                                    SelIndexList.append((ti, fi))
                        else:
                            for i in range(len(dict_[idx][0][1])):
                                param = dict_[idx][0][1][i]
                                TableIndex, FieldIndex, FieldType, isTrue = GetFilterParam(tableName_Order, current_field, param)
                                if not isTrue:
                                    return [], [], False
                                SelIndexList.append((TableIndex, FieldIndex))
                        tmp_List = current_list[:]
                        current_list = []
                        for tmpRecord in tmp_List:
                            tmp = []
                            for x in SelIndexList:
                                if len(current_field) == 1:
                                    tmp.append(tmpRecord[x[1]])
                                else:
                                    tmp.append(tmpRecord[x[0]][x[1]])
                            current_list.append(tmp)
                        outPutField = []
                        for xi in SelIndexList:
                            field_name = current_field[xi[0]][xi[1]][0]
                            if isinstance(field_name, bytes):
                                field_name = field_name.decode('utf-8')
                            outPutField.append(
                                tableName_Order[xi[0]].strip() + '.' + field_name.strip()
                            )
                        return outPutField, current_list, True
                idx -= 1
        
        outPutField, current_list, isRight = excute_tree()
        if isRight:
            print('-' * (len(outPutField) * 12))
            print(' | '.join(outPutField))
            print('-' * (len(outPutField) * 12))
            for record in current_list:
                row = []
                for item in record:
                    if isinstance(item, bytes):
                        row.append(item.decode('utf-8').strip())
                    else:
                        row.append(str(item).strip())
                print(' | '.join(row))
                print('-' * (len(outPutField) * 12))
        else:
            print('WRONG SQL INPUT!')
    else:
        print('there is no query plan tree for the execution')

def construct_logical_tree():
    syn_tree = common_db.global_syn_tree
    if syn_tree:
        if syn_tree.value == 'SFW':
            sel_list, from_list, where_list = extract_sfw_data()
            sel_list = [i for i in sel_list if i != ',']
            from_list = [i for i in from_list if i != ',']
            where_list = tuple(where_list)
            from_node = construct_from_node(from_list)
            where_node = construct_where_node(from_node, where_list)
            common_db.global_logical_tree = construct_select_node(where_node, sel_list)
        else:
            common_db.global_logical_tree = syn_tree
    else:
        print('there is no data in the syntax tree in the construct_logical_tree')

# ----------------------------------------------
# Author: Xinjian Zhang
# modified by: Ruizhe Yang   419198812@qq.com
# to execute CREATE TABLE SQL statements
# input
#       syn_tree: syntax tree node for CREATE TABLE
#       schema_obj: schema object to manage table definitions
# output
#       None (creates table in schema and storage)
# ------------------------------------------------
def execute_create_table(syn_tree, schema_obj=None):
    print("Executing CREATE TABLE statement...")
    if syn_tree.value == 'CREATE_TABLE':
        table_name = syn_tree.var['table_name']
        fields = syn_tree.var['fields']
        if schema_obj is None:
            import schema_db
            schema_obj = schema_db.Schema()
        table_name_bytes = table_name.encode('utf-8')
        if schema_obj.find_table(table_name_bytes):
            print(f"Table '{table_name}' already exists!")
            return
        try:
            # 1. 生成事务ID并登记活动事务
            tx_id = str(uuid.uuid4())
            print("写入活动事务日志")
            log_db.LogManager.add_active_tx(tx_id)
            # 2. 前像日志（表不存在，前像为None）
            print("写入前像日志")
            log_db.LogManager.log_before_image(tx_id, table_name, None)
            # 3. 创建表（写schema和数据文件）
            field_list = []
            for field_def in fields:
                field_name = field_def['name']
                type_code = field_def['type_code']
                length = field_def['length']
                field_name_padded = (' ' * (10 - len(field_name)) + field_name) if len(field_name) < 10 else field_name[:10]
                field_list.append((field_name_padded.encode('utf-8'), type_code, length))
            schema_obj.appendTable(table_name_bytes, field_list)
            import storage_db
            storage_obj = storage_db.Storage(table_name_bytes, field_list_from_create_table=fields)
            # 4. 后像日志（表结构）
            log_db.LogManager.log_after_image(tx_id, table_name, fields)
            # 5. 提交事务日志
            log_db.LogManager.add_commit_tx(tx_id)
            print(f"Table '{table_name}' created successfully!")
        except Exception as e:
            print(f"Error creating table: {e}")

# ----------------------------------------------
# Author: Xinjian Zhang
# to execute INSERT INTO SQL statements
# input
#       syn_tree: syntax tree node for INSERT INTO
#       schema_obj: schema object (optional)
# output
#       None (inserts record into table)
# ------------------------------------------------
def execute_insert_into(syn_tree, schema_obj=None):
    if syn_tree.value == 'INSERT_INTO':
        table_name = syn_tree.var['table_name']
        values = syn_tree.var['values']
        
        try:
            table_name_bytes = table_name.encode('utf-8')
            storage_obj = storage_db.Storage(table_name_bytes)
            if storage_obj.insert_record(values):
                print(f"Record inserted into '{table_name}' successfully!")
            else:
                print(f"Failed to insert record into '{table_name}'!")
        except Exception as e:
            print(f"Error inserting record: {e}")

# ----------------------------------------------
# Author: Xinjian Zhang
# to execute DELETE FROM SQL statements
# input
#       syn_tree: syntax tree node for DELETE FROM
#       schema_obj: schema object (optional)
# output
#       None (deletes records from table)
# ------------------------------------------------
def execute_delete_from(syn_tree, schema_obj=None):
    if syn_tree.value == 'DELETE_FROM':
        table_name = syn_tree.var['table_name']
        condition = syn_tree.var.get('condition')
        
        try:
            table_name_bytes = table_name.encode('utf-8')
            storage_obj = storage_db.Storage(table_name_bytes)
            
            if condition is None:
                # Delete all records
                storage_obj.record_list = []
                storage_obj._rewrite_data_file()
                print(f"All records deleted from '{table_name}'!")
            else:
                # Delete based on condition
                field_name = condition.children[0].children[0]  # TCNAME
                value = condition.children[2].children[0]       # CONSTANT
                
                # Remove quotes from value if present
                if isinstance(value, str) and value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                
                storage_obj.delete_record_by_field(field_name, value)
                print(f"Records deleted from '{table_name}' where {field_name}='{value}'!")
        except Exception as e:
            print(f"Error deleting from table: {e}")

# ----------------------------------------------
# Author: Xinjian Zhang
# to execute UPDATE SET SQL statements
# input
#       syn_tree: syntax tree node for UPDATE SET
#       schema_obj: schema object (optional)
# output
#       None (updates records in table)
# ------------------------------------------------
def execute_update_set(syn_tree, schema_obj=None):
    if syn_tree.value == 'UPDATE_SET':
        table_name = syn_tree.var['table_name']
        assignments = syn_tree.var['assignments']
        condition = syn_tree.var.get('condition')
        
        try:
            table_name_bytes = table_name.encode('utf-8')
            storage_obj = storage_db.Storage(table_name_bytes)
            
            # Process each assignment
            for assignment in assignments:
                update_field = assignment['column']
                new_value = assignment['value']
                
                # Remove quotes from new_value if present
                if isinstance(new_value, str) and new_value.startswith("'") and new_value.endswith("'"):
                    new_value = new_value[1:-1]
                
                if condition:
                    # Update based on condition
                    condition_field = condition.children[0].children[0]  # TCNAME
                    condition_value = condition.children[2].children[0]  # CONSTANT
                    
                    # Remove quotes from condition_value if present
                    if isinstance(condition_value, str) and condition_value.startswith("'") and condition_value.endswith("'"):
                        condition_value = condition_value[1:-1]
                    
                    # Handle case where update field differs from condition field
                    if update_field == condition_field:
                        # If update field and condition field are the same, use existing method
                        storage_obj.update_record_by_field(condition_field, condition_value, new_value)
                    else:
                        # Custom update logic for different fields
                        updated = False
                        
                        # Find field indices
                        condition_field_index = None
                        update_field_index = None
                        
                        for idx, field in enumerate(storage_obj.field_name_list):
                            field_name = field[0].decode('utf-8').strip() if isinstance(field[0], bytes) else str(field[0]).strip()
                            if field_name == condition_field:
                                condition_field_index = idx
                            if field_name == update_field:
                                update_field_index = idx
                        
                        if condition_field_index is not None and update_field_index is not None:
                            # Iterate through records to find matching condition
                            for i, record in enumerate(storage_obj.record_list):
                                record_value = record[condition_field_index]
                                if isinstance(record_value, bytes):
                                    record_value = record_value.decode('utf-8').strip()
                                else:
                                    record_value = str(record_value).strip()
                                
                                if record_value == condition_value:
                                    # Found matching record, update target field
                                    record_list = list(record)
                                    
                                    # Handle field type for new value
                                    field_type = storage_obj.field_name_list[update_field_index][1]
                                    field_length = storage_obj.field_name_list[update_field_index][2]
                                    
                                    if field_type == 0 or field_type == 1:  # String types
                                        # Ensure new value fits field length
                                        padded_value = new_value.ljust(field_length)[:field_length]
                                        record_list[update_field_index] = padded_value.encode('utf-8')
                                    elif field_type == 2:  # Integer type
                                        record_list[update_field_index] = int(new_value)
                                    elif field_type == 3:  # Boolean type
                                        record_list[update_field_index] = bool(new_value)
                                    
                                    storage_obj.record_list[i] = tuple(record_list)
                                    updated = True
                                    print(f"Updated record: {condition_field}='{condition_value}' -> {update_field}='{new_value}'")
                                    break
                            
                            if updated:
                                storage_obj._rewrite_data_file()
                            else:
                                print("No matching record found.")
                        else:
                            print(f"Field not found: {condition_field} or {update_field}")
                else:
                    # Update all records (no WHERE condition)
                    print("Update without WHERE clause not implemented for safety!")
                
            print(f"Records updated in '{table_name}'!")
        except Exception as e:
            print(f"Error updating table: {e}")

# ----------------------------------------------
# Author: Xinjian Zhang
# modified by: Ruizhe Yang   419198812@qq.com
# to execute DROP TABLE SQL statements
# input
#       syn_tree: syntax tree node for DROP TABLE
#       schema_obj: schema object (optional)
# output
#       None (drops table from schema and storage)
# ------------------------------------------------
def execute_drop_table(syn_tree, schema_obj=None):
    if syn_tree.value == 'DROP_TABLE':
        table_name = syn_tree.var['table_name']
        if schema_obj is None:
            import schema_db
            schema_obj = schema_db.Schema()
        table_name_bytes = table_name.encode('utf-8')
        if not schema_obj.find_table(table_name_bytes):
            print(f"Table '{table_name}' does not exist!")
            return
        try:
            # 1. 生成事务ID并登记活动事务
            tx_id = str(uuid.uuid4())
            log_db.LogManager.add_active_tx(tx_id)
            # 2. 前像日志（删除前，记录表结构）
            fields = schema_obj.headObj.tableFields.get(table_name_bytes, None)
            log_db.LogManager.log_before_image(tx_id, table_name, fields)
            # 3. 删除表（schema和数据文件）
            schema_obj.delete_table_schema(table_name_bytes)
            import storage_db
            storage_obj = storage_db.Storage(table_name_bytes)
            storage_obj.delete_table_data(table_name_bytes)
            # 4. 后像日志（表已不存在，后像为None）
            log_db.LogManager.log_after_image(tx_id, table_name, None)
            # 5. 提交事务日志
            log_db.LogManager.add_commit_tx(tx_id)
            print(f"Table '{table_name}' dropped successfully!")
        except Exception as e:
            print(f"Error dropping table: {e}")

# ----------------------------------------------
# Author: Xinjian Zhang
# unified entry point for executing SQL statements
# input
#       schema_obj: schema object to manage table definitions
# output
#       None (executes appropriate SQL operation)
# ------------------------------------------------
def execute_sql_statement(schema_obj=None):
    """Unified entry point for executing SQL statements"""
    syn_tree = common_db.global_syn_tree
    if not syn_tree:
        print("No syntax tree to execute!")
        return
    
    if syn_tree.value == 'SFW':
        construct_logical_tree()
        execute_logical_tree()
    elif syn_tree.value == 'CREATE_TABLE':
        execute_create_table(syn_tree, schema_obj)
    elif syn_tree.value == 'INSERT_INTO':
        execute_insert_into(syn_tree, schema_obj)
    elif syn_tree.value == 'DELETE_FROM':
        execute_delete_from(syn_tree, schema_obj)
    elif syn_tree.value == 'UPDATE_SET':
        execute_update_set(syn_tree, schema_obj)
    elif syn_tree.value == 'DROP_TABLE':
        execute_drop_table(syn_tree, schema_obj)
    else:
        print(f"Unsupported SQL statement type: {syn_tree.value}")