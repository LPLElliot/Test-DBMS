'''
index_db.py
索引管理模块
实现B树索引和哈希索引的创建、删除和搜索功能
'''
import os
import struct
import ctypes
import traceback
import common_db
import storage_db

# 常量定义
BTREE_INDEX = 1    # B树索引类型
HASH_INDEX = 2     # 哈希索引类型 
LEAF_NODE_TYPE = 0 # 叶节点
INTERNAL_NODE_TYPE = 1  # 内部节点
SPECIAL_BLOCK_PTR = -1  # 特殊块指针

# ----------------------------------------------
# Author: WuShuCheng
# 索引类
# 功能:
#   - 初始化索引对象
#   - 创建B树/哈希索引
#   - 索引查询
#   - 删除索引
# ----------------------------------------------
class Index(object):
    def __init__(self, tablename):
        """
        初始化索引对象
        参数:
            tablename: 表名
        """
        print("__init__ of", Index.__name__)
        self.table_name = tablename.strip()
        self.has_root = False
        self.num_of_levels = 0
        self.root_node_ptr = -1
        self.current_block_id = 1
        
        try:
            # 初始化first_block_buf
            self.first_block_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            
            if not os.path.exists(tablename + '.ind'):
                print('index file ' + tablename + '.ind does not exist')
                self.f_handle = open(tablename + '.ind', 'wb+')
                print(tablename + '.ind has been created')
                # 初始化索引文件头
                struct.pack_into('!i?ii', self.first_block_buf, 0, 0, False, 0, -1)
                self.f_handle.write(self.first_block_buf)
            else:
                self.f_handle = open(tablename + '.ind', 'rb+')
                print('index file ' + tablename + '.ind has been opened')
                # 读取索引文件头
                self.f_handle.seek(0)
                self.f_handle.readinto(self.first_block_buf)
                _, self.has_root, self.num_of_levels, self.root_node_ptr = \
                    struct.unpack_from('!i?ii', self.first_block_buf, 0)
        except Exception as e:
            print(f"初始化索引时出错: {str(e)}")
            raise
# ----------------------------------------------
# Author: WuShuCheng
# 创建索引
# 参数:
#   field_name: 字段名
#   index_type: 索引类型(BTREE_INDEX/HASH_INDEX)
# 返回:
#   bool: 创建是否成功
# ----------------------------------------------
    def create_index(self, field_name):
        """创建索引"""
        try:
            print("开始创建索引...")
            storage_obj = storage_db.Storage(self.table_name.encode('utf-8'))
            field_list = storage_obj.getFieldList()
            
            # 找到目标字段
            field_index = -1
            field_type = None
            if isinstance(field_name, str):
                field_name = field_name.encode('utf-8')
            
            for i, field in enumerate(field_list):
                if field[0].strip() == field_name.strip():
                    field_index = i
                    field_type = field[1]
                    break
        
            if field_index == -1:
                print(f"未找到字段: {field_name.decode('utf-8')}")
                return False
                
            # 读取记录
            records = []
            for block_id in range(1, storage_obj.data_block_num + 1):
                storage_obj.f_handle.seek(block_id * common_db.BLOCK_SIZE)
                block = storage_obj.f_handle.read(common_db.BLOCK_SIZE)
                
                num_records = struct.unpack('!i', block[:4])[0]
                curr_offset = 4
                
                print(f"处理数据块 {block_id}, 包含 {num_records} 条记录")
                
                for _ in range(num_records):
                    record = []
                    offset = curr_offset
                    
                    # 读取每个字段
                    for field in field_list:
                        field_len = field[2]
                        field_data = block[offset:offset+field_len]
                        record.append(field_data)
                        offset += field_len
                    
                    # 获取索引字段值
                    key = record[field_index]
                    if field_type == 2:  # 整数类型
                        value_int = struct.unpack('!q', key)[0]
                        key = struct.pack('!q', value_int)
                        print(f"添加索引项: {value_int}")
                    else:
                        print(f"添加索引项: {key.decode('utf-8', 'ignore')}")
                    
                    records.append((key, block_id, curr_offset))
                    curr_offset = offset
        
            # 排序记录
            if field_type == 2:
                records.sort(key=lambda x: struct.unpack('!q', x[0])[0])
            else:
                records.sort(key=lambda x: x[0])
            
            # 创建索引文件
            self.has_root = True
            self.num_of_levels = 1
            self.root_node_ptr = 1
            
            # 写入根节点
            root_node = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            struct.pack_into('!ii', root_node, 0, LEAF_NODE_TYPE, len(records))
            
            offset = 8
            for key, block_id, rec_offset in records:
                struct.pack_into('!8sii', root_node, offset, key, block_id, rec_offset)
                offset += 16
            
            self.f_handle.seek(common_db.BLOCK_SIZE)
            self.f_handle.write(root_node)
            
            # 更新文件头
            struct.pack_into('!i?iii', self.first_block_buf, 0, 1, True, 1, 1, BTREE_INDEX)
            self.f_handle.seek(0)
            self.f_handle.write(self.first_block_buf)
            
            print(f"索引创建成功! 共 {len(records)} 条记录")
            return True
            
        except Exception as e:
            print(f"创建索引时出错: {str(e)}")
            traceback.print_exc()
            return False

    # ----------------------------------------------
    # Author: WuShuCheng  
    # 索引查询
    # 参数:
    #   field_name: 字段名
    #   search_value: 搜索值
    # 返回:
    #   list: 匹配记录列表[(block_id, offset),...]
    # ----------------------------------------------
    def search_by_index(self, field_name, search_value):
        """使用索引搜索记录"""
        try:
            if not self.has_root:
                print("索引为空")
                return []
            
            # 获取字段类型
            storage_obj = storage_db.Storage(self.table_name.encode('utf-8'))
            field_list = storage_obj.getFieldList()
            field_type = None
            
            for field in field_list:
                if field[0].strip() == field_name.encode('utf-8').strip():
                    field_type = field[1]
                    break
        
            # 准备搜索键值
            if field_type == 2:  # 整数类型
                search_int = int(search_value)
                search_key = struct.pack('!q', search_int)
                print(f"搜索整数值: {search_int}")
            else:
                if isinstance(search_value, str):
                    search_key = search_value.encode('utf-8')
                print(f"搜索字符串: {search_value}")
        
            # 读取根节点
            node_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            self.f_handle.seek(self.root_node_ptr * common_db.BLOCK_SIZE)
            self.f_handle.readinto(node_buf)
            
            node_type, num_keys = struct.unpack_from('!ii', node_buf, 0)
            print(f"索引节点包含 {num_keys} 个键值")
            
            # 搜索记录
            results = []
            offset = 8
            for i in range(num_keys):
                key, block_id, record_offset = struct.unpack_from('!8sii', node_buf, offset)
                
                if field_type == 2:
                    key_int = struct.unpack('!q', key)[0]
                    print(f"比较: {key_int} vs {struct.unpack('!q', search_key)[0]}")
                    if key_int == struct.unpack('!q', search_key)[0]:
                        results.append((block_id, record_offset))
                        print(f"找到匹配记录: 块号={block_id}, 偏移量={record_offset}")
                else:
                    if key.strip() == search_key.strip():
                        results.append((block_id, record_offset))
                
                offset += 16
            
            print(f"共找到 {len(results)} 条记录")
            return results
            
        except Exception as e:
            print(f"索引搜索出错: {str(e)}")
            traceback.print_exc()
            return []

    # ----------------------------------------------
    # Author: WuShuCheng
    # 创建B树索引
    # 参数:
    #   records: 记录列表[(key, block_id, offset),...]
    #   field_type: 字段类型
    # 返回:
    #   bool: 创建是否成功
    # ----------------------------------------------
    def _create_btree_index(self, records, field_type):
        """创建B树索引"""
        try:
            # 排序记录
            if field_type == 2:  # 整数类型
                records.sort(key=lambda x: struct.unpack('!q', x[0])[0])
            else:
                records.sort(key=lambda x: x[0])
            
            print(f"创建B树索引, 共 {len(records)} 条记录")
            for key, block_id, offset in records:
                if field_type == 2:
                    key_int = struct.unpack('!q', key)[0]
                    print(f"索引项: 键值={key_int}, 块号={block_id}, 偏移量={offset}")
                else:
                    print(f"索引项: 键值={key.decode('utf-8', 'ignore')}, "
                          f"块号={block_id}, 偏移量={offset}")
            
            # 创建根节点
            root_node = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            struct.pack_into('!ii', root_node, 0, LEAF_NODE_TYPE, len(records))
            
            # 写入索引记录
            offset = 8
            for key, block_id, rec_offset in records:
                struct.pack_into('!8sii', root_node, offset, key, block_id, rec_offset)
                offset += 16
            
            # 写入根节点
            self.has_root = True
            self.num_of_levels = 1
            self.root_node_ptr = 1
            
            self.f_handle.seek(common_db.BLOCK_SIZE)
            self.f_handle.write(root_node)
            
            # 更新文件头
            struct.pack_into('!i?iii', self.first_block_buf, 0, 1, True, 1, 1, BTREE_INDEX)
            self.f_handle.seek(0)
            self.f_handle.write(self.first_block_buf)
            
            return True
            
        except Exception as e:
            print(f"创建B树索引时出错: {str(e)}")
            traceback.print_exc()
            return False

    # ----------------------------------------------
    # Author: WuShuCheng
    # B树搜索
    # 参数:
    #   search_key: 搜索键值
    #   field_type: 字段类型
    # 返回:
    #   list: 匹配记录列表[(block_id, offset),...]
    # ----------------------------------------------
    def _search_btree(self, search_key, field_type):
        """B树索引搜索"""
        try:
            print(f"\n使用B树索引搜索...")
            
            # 读取根节点
            node_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            self.f_handle.seek(self.root_node_ptr * common_db.BLOCK_SIZE)
            self.f_handle.readinto(node_buf)
            
            node_type, num_keys = struct.unpack_from('!ii', node_buf, 0)
            print(f"根节点包含 {num_keys} 个键值")
            
            # 搜索记录
            results = []
            offset = 8
            for i in range(num_keys):
                key, block_id, record_offset = struct.unpack_from('!8sii', node_buf, offset)
                
                # 比较键值
                if field_type == 2:
                    key_int = struct.unpack('!q', key)[0]
                    search_int = struct.unpack('!q', search_key)[0]
                    print(f"比较键值: {key_int} vs {search_int}")
                    if key_int == search_int:
                        results.append((block_id, record_offset))
                        print(f"找到匹配记录: 块号={block_id}, 偏移量={record_offset}")
                else:
                    if key.strip(b'\x00') == search_key.strip(b'\x00'):
                        results.append((block_id, record_offset))
                        print(f"找到匹配记录: 块号={block_id}, 偏移量={record_offset}")
                
                offset += 16
            
            print(f"共找到 {len(results)} 条记录")
            return results
            
        except Exception as e:
            print(f"B树索引搜索出错: {str(e)}")
            traceback.print_exc()
            return []

    def __del__(self):
        """析构函数,关闭文件句柄"""
        print("__del__ of", Index.__name__)
        if hasattr(self, 'f_handle'):
            self.f_handle.close()