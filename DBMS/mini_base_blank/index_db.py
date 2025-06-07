'''
index_db.py
在此模块中实现B树索引
'''
import struct
import os
import ctypes
import common_db
import storage_db  # 添加这行导入

# 第0块存储树的元信息
'''
block_id|has_root|num_of_levels|root_node_ptr
# 注意：root_node_ptr是一个块id
'''

MAX_NUM_OF_KEYS=200#the number of keys in each block
# structure of leaf node
'''
block_id|node_type|number_of_keys|key_0|ptr_0|...|key_i|ptr_i|...|key_n|ptr_n|...free space...|last_ptr
note: for leaf node, ptr is a block id+entry id (8 bytes) except for the last one
'''
LEAF_NODE_TYPE=1
LEN_OF_LEAF_NODE=10+4+4  # key takes 10 bytes, block_id takes 4 bytes and offset takes 4 bytes
# structure of internal node
'''
block_id|node_type|number_of_keys|key_0|ptr_0|key_1|ptr_1|...|key_n|ptr_n|...free space...|last_ptr|
note: For internal node, ptr is just a block id( 4 bytes) 
'''
INTERNAL_NODE_TYPE=0
SPECIAL_INDEX_BLOCK_PTR=-1 # this is the last ptr for last leaf node when the next node is unknown

def test():
    my_dict={}
    my_dict.setdefault('one',80)
    my_dict.setdefault('two',90)
    my_dict.setdefault('aaa',90)
    print (my_dict.keys())
    print (my_dict.items())
    for my_each_key in sorted(my_dict):
        print ("the value of key ",my_each_key," is ",my_dict[my_each_key])
    my_list=[]
    my_tuple=(1,2)
    my_list.append(my_tuple)
    (a,b)=my_list[0]
    print (a,b)
    
class Index(object):
    def __init__(self, tablename):
        print("__init__ of ", Index.__name__)
        self.table_name = tablename.strip()
        self.has_root = False
        self.num_of_levels = 0
        self.root_node_ptr = -1
        self.first_block_buf = None
        
        if not os.path.exists(tablename + '.ind'):
            print('index file ' + tablename + '.ind does not exist')
            self.f_handle = open(tablename + '.ind', 'wb+')
            print(tablename + '.ind has been created')
            # 初始化第一个块作为元数据块
            self.first_block_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            struct.pack_into('!i?ii', self.first_block_buf, 0, 0, False, 0, -1)
            self.f_handle.write(self.first_block_buf)
        else:
            self.f_handle = open(tablename + '.ind', 'rb+')
            print('index file ' + tablename + '.ind has been opened')
            # 读取第一个块的元数据
            self.first_block_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            self.f_handle.seek(0)
            self.f_handle.readinto(self.first_block_buf)
            # 解析元数据
            block_id, self.has_root, self.num_of_levels, self.root_node_ptr = struct.unpack_from('!i?ii', self.first_block_buf, 0)

    def create_index(self, field_name):
        """创建索引"""
        print('create_index begins to execute')
        storage_obj = storage_db.Storage(self.table_name.encode('utf-8'))
        
        try:
            # 获取字段列表
            field_list = storage_obj.getFieldList()
            print("Available fields:", [f[0].decode('utf-8').strip() for f in field_list])
            
            # 找到目标字段的索引
            field_index = -1
            for i, field in enumerate(field_list):
                if field[0].strip().decode('utf-8') == field_name.strip():
                    field_index = i
                    break
                    
            if field_index == -1:
                print(f"Field {field_name} not found in field list")
                return False
            
            # 获取所有记录
            records = storage_obj.getRecord()
            print(f"Total records to index: {len(records)}")
            
            # 插入所有记录到索引
            for block_id, offset, record in records:
                field_value = record[field_index]
                if isinstance(field_value, bytes):
                    field_value = field_value.strip()
                self.insert_index_entry(field_value, block_id, offset)
                
            print("Index creation completed")
            return True
            
        except Exception as e:
            print(f"Error creating index: {str(e)}")
            return False
        finally:
            if 'storage_obj' in locals():
                del storage_obj

    def search_by_index(self, field_name, search_value):
        """使用索引搜索"""
        if not self.has_root:
            print("Index is empty")
            return []
            
        try:
            results = []
            current_node = self.root_node_ptr
            
            # 转换搜索值格式
            if isinstance(search_value, str):
                search_value = search_value.encode('utf-8')
                
            while current_node != SPECIAL_INDEX_BLOCK_PTR:
                node_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
                self.f_handle.seek(current_node * common_db.BLOCK_SIZE)
                self.f_handle.readinto(node_buf)
                
                node_type, num_keys = struct.unpack_from('!ii', node_buf, 0)
                
                if node_type == LEAF_NODE_TYPE:
                    # 在叶子节点中搜索
                    for i in range(num_keys):
                        key, block_id, offset = struct.unpack_from('!10sii', node_buf, 
                            struct.calcsize('!iii') + i * LEN_OF_LEAF_NODE)
                        if key.strip() == search_value.strip():
                            results.append((block_id, offset))
                    break
                else:
                    # 在内部节点中查找下一个要访问的节点
                    current_node = self.get_next_block_ptr(search_value, [], [])
                    
            return results
            
        except Exception as e:
            print(f"Error during index search: {str(e)}")
            return []

    def __del__(self):
        """析构函数"""
        print("__del__ of ", Index.__name__)
        if hasattr(self, 'f_handle'):
            self.f_handle.close()