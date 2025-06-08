# ------------------------------------------------
# index_db.py
# Author: Xinjian Zhang 278254081@qq.com
# Modified by: WuShuCheng 2396878284@qq.com
# ------------------------------------------------
# Index management module
# Implements B-tree and Hash index creation, deletion and search functionality
# ------------------------------------------------

import os
import struct
import ctypes
import common_db
import storage_db

# Constants definitions
BTREE_INDEX = 1        # B-tree index type
HASH_INDEX = 2         # Hash index type 
LEAF_NODE_TYPE = 0     # Leaf node type
INTERNAL_NODE_TYPE = 1 # Internal node type

# B-tree configuration
BTREE_ORDER = 64       # B-tree order (number of keys per node)
HASH_TABLE_SIZE = 1024 # Hash table size

# ------------------------------------------------
# Index class for managing database indexes
# Author: Xinjian Zhang 278254081@qq.com
# Functionality:
#   - Initialize index object
#   - Create B-tree/Hash index
#   - Index search operations
#   - Delete index files
# ------------------------------------------------
class Index(object):
    
    # ------------------------------------------------
    # Constructor for Index class
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       tablename: name of the table to create index for
    #       index_type: type of index (BTREE_INDEX or HASH_INDEX)
    # Output:
    #       Index object instance
    # ------------------------------------------------
    def __init__(self, tablename, index_type=BTREE_INDEX):
        print(f"Initializing {['', 'B-tree', 'Hash'][index_type]} index for table: {tablename}")
        self.table_name = tablename.strip()
        self.index_type = index_type
        self.has_root = False
        self.num_of_levels = 0
        self.root_node_ptr = -1
        self.current_block_id = 1
        self.hash_table_size = HASH_TABLE_SIZE if index_type == HASH_INDEX else 0
        self.field_list = []
        self.max_key_length = 8  
    
        try:
            storage_obj = storage_db.Storage(tablename.encode('utf-8'), debug=False)
            self.field_list = storage_obj.getFieldList()
            del storage_obj
            
            self.first_block_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            index_filename = tablename + ('.hash' if index_type == HASH_INDEX else '.ind')
            
            if not os.path.exists(index_filename):
                print(f'Creating new index file: {index_filename}')
                self.f_handle = open(index_filename, 'wb+')
                if index_type == HASH_INDEX:
                    struct.pack_into('!i?iii', self.first_block_buf, 0, 0, False, 0, -1, HASH_INDEX)
                else:
                    struct.pack_into('!i?iii', self.first_block_buf, 0, 0, False, 0, -1, BTREE_INDEX)
                self.f_handle.write(self.first_block_buf)
                self.f_handle.flush()
            else:
                print(f'Opening existing index file: {index_filename}')
                self.f_handle = open(index_filename, 'rb+')
                # Read index file header
                self.f_handle.seek(0)
                self.f_handle.readinto(self.first_block_buf)
                _, self.has_root, self.num_of_levels, self.root_node_ptr, self.index_type = \
                    struct.unpack_from('!i?iii', self.first_block_buf, 0)
                print(f"Index loaded - type: {self.index_type}, has_root: {self.has_root}, levels: {self.num_of_levels}")
        except Exception as e:
            print(f"Error initializing index: {str(e)}")
            self.field_list = []
            raise

    # ------------------------------------------------
    # Create index on specified field
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       field_name: name of the field to create index on
    #       index_type: optional index type override
    # Output:
    #       boolean indicating success/failure
    # ------------------------------------------------
    def create_index(self, field_name, index_type=None):
        if index_type:
            self.index_type = index_type  
        try:
            # Get storage object and field information
            storage_obj = storage_db.Storage(self.table_name.encode('utf-8'), debug=False)
            field_list = storage_obj.getFieldList()
            
            # Find target field
            field_index = -1
            field_type = None
            target_field = field_name.strip()
            
            for i, field in enumerate(field_list):
                field_name_in_list = field[0].decode('utf-8').strip() if isinstance(field[0], bytes) else str(field[0]).strip()
                if field_name_in_list == target_field:
                    field_index = i
                    field_type = field[1]
                    break
            
            if field_index == -1:
                print(f"Field not found: '{field_name}'")
                print(f"Available fields: {[f[0].decode('utf-8').strip() if isinstance(f[0], bytes) else str(f[0]).strip() for f in field_list]}")
                return False      
            
            # Collect all records
            records = self._collect_records(storage_obj, field_list, field_index, field_type)            
            if not records:
                print("No records collected for indexing")
                return False           
            print(f"Successfully collected {len(records)} records")           
            
            # Create index based on type
            if self.index_type == HASH_INDEX:
                return self._create_hash_index(records, field_type)
            else:
                return self._create_btree_index(records, field_type)     
        except Exception as e:
            print(f"Error creating index: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    # ------------------------------------------------
    # Collect records from table for indexing
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       storage_obj: storage object for table access
    #       field_list: list of field definitions
    #       field_index: index of field to create index on
    #       field_type: data type of the field
    # Output:
    #       list of (key, block_id, offset) tuples
    # ------------------------------------------------
    def _collect_records(self, storage_obj, field_list, field_index, field_type):
        records = []
        storage_obj.f_handle.seek(0)
        header_block = storage_obj.f_handle.read(common_db.BLOCK_SIZE)
        
        if len(header_block) < 12:
            print("Error: Data file header too small")
            return []
        
        block_id, data_block_num = struct.unpack('!ii', header_block[:8])
        if data_block_num == 0:
            print("No data blocks found")
            return []
        
        for block_id in range(1, data_block_num + 1):
            storage_obj.f_handle.seek(block_id * common_db.BLOCK_SIZE)
            block = storage_obj.f_handle.read(common_db.BLOCK_SIZE)
            if len(block) < 8:
                continue  
            
            block_header, num_records = struct.unpack('!ii', block[:8])
            if num_records == 0:
                continue
            
            # Read record offsets
            offsets = []
            for i in range(num_records):
                offset_pos = 8 + i * 4
                if offset_pos + 4 <= len(block):
                    offset = struct.unpack('!i', block[offset_pos:offset_pos + 4])[0]
                    offsets.append(offset)
            
            # Process each record
            for rec_idx, start_offset in enumerate(offsets):
                if start_offset + 18 > len(block):
                    continue     
                
                record_data_start = start_offset + 18
                curr_offset = record_data_start
                record = []
                
                # Extract field data
                for field_idx, field in enumerate(field_list):
                    field_len = field[2]
                    if curr_offset + field_len > len(block):
                        break
                    field_data = block[curr_offset:curr_offset + field_len]
                    record.append(field_data)
                    curr_offset += field_len
                
                if len(record) <= field_index:
                    continue   
                
                # Process key based on field type
                key = record[field_index]
                if field_type == 2:  # Integer type
                    try:
                        if isinstance(key, bytes):
                            key_str = key.decode('utf-8', 'ignore').strip()
                            if key_str.isdigit():
                                value_int = int(key_str)
                            else:
                                if len(key) >= 4:
                                    value_int = struct.unpack('!i', key[:4])[0]
                                else:
                                    continue
                        elif isinstance(key, (int, str)):
                            value_int = int(key)
                        else:
                            continue
                        key = struct.pack('!q', value_int)
                        
                    except Exception as e:
                        continue
                else:  # String/VARSTRING type
                    if len(key) > 0:
                        key_str = key.decode('utf-8', 'ignore').rstrip('\x00').strip()
                        key = key_str.encode('utf-8')[:8].ljust(8, b'\x00')
                    else:
                        continue
                
                records.append((key, block_id, start_offset))
        
        print(f"Successfully collected {len(records)} records for indexing")
        return records

    # ------------------------------------------------
    # Create B-tree index from collected records
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       records: list of (key, block_id, offset) tuples
    #       field_type: data type of the indexed field
    # Output:
    #       boolean indicating success/failure
    # ------------------------------------------------
    def _create_btree_index(self, records, field_type):
        try:
            print(f"Creating B-tree index with {len(records)} records")
            
            # Sort records by key
            if field_type == 2:  # Integer type
                records.sort(key=lambda x: struct.unpack('!q', x[0])[0])
            else:  # String type
                records.sort(key=lambda x: x[0])
            
            # Calculate optimal tree structure
            max_keys_per_node = (common_db.BLOCK_SIZE - 8) // 16
            
            # Create leaf nodes
            leaf_nodes = []
            current_block_id = 1
            for i in range(0, len(records), max_keys_per_node):
                chunk = records[i:i + max_keys_per_node]
                leaf_node = self._create_leaf_node(chunk, current_block_id)
                leaf_nodes.append((current_block_id, leaf_node))
                current_block_id += 1
            
            # Set root
            if len(leaf_nodes) == 1:
                self.has_root = True
                self.num_of_levels = 1
                self.root_node_ptr = leaf_nodes[0][0]
            else:
                # Create internal nodes
                self.root_node_ptr = self._create_internal_nodes(leaf_nodes, current_block_id, field_type)
                self.has_root = True
            
            # Write all nodes to file
            for block_id, node_data in leaf_nodes:
                self.f_handle.seek(block_id * common_db.BLOCK_SIZE)
                self.f_handle.write(node_data)
            
            # Update file header
            struct.pack_into('!i?iii', self.first_block_buf, 0, current_block_id - 1, 
                           True, self.num_of_levels, self.root_node_ptr, BTREE_INDEX)
            self.f_handle.seek(0)
            self.f_handle.write(self.first_block_buf)
            self.f_handle.flush()
            
            print(f"✓ B-tree index created: {self.num_of_levels} levels, root at block {self.root_node_ptr}")
            return True
        except Exception as e:
            print(f"✗ Error creating B-tree index: {str(e)}")
            return False

    # ------------------------------------------------
    # Create leaf node for B-tree
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       records: list of records for this leaf node
    #       block_id: block ID for this node
    # Output:
    #       buffer containing leaf node data
    # ------------------------------------------------
    def _create_leaf_node(self, records, block_id):
        node = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
        struct.pack_into('!ii', node, 0, LEAF_NODE_TYPE, len(records))
        offset = 8
        for key, data_block_id, rec_offset in records:
            struct.pack_into('!8sii', node, offset, key, data_block_id, rec_offset)
            offset += 16
        return node

    # ------------------------------------------------
    # Create internal nodes for B-tree
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       child_nodes: list of child node information
    #       start_block_id: starting block ID for internal nodes
    #       field_type: data type of the indexed field
    # Output:
    #       block ID of the root node
    # ------------------------------------------------
    def _create_internal_nodes(self, child_nodes, start_block_id, field_type):
        if len(child_nodes) == 1:
            return child_nodes[0][0]
        
        max_children_per_node = (common_db.BLOCK_SIZE - 8) // 12  # 8 bytes key + 4 bytes pointer
        internal_nodes = []
        current_block_id = start_block_id
        
        for i in range(0, len(child_nodes), max_children_per_node):
            chunk = child_nodes[i:i + max_children_per_node]
            internal_node = self._create_internal_node(chunk, field_type)
            internal_nodes.append((current_block_id, internal_node))
            
            # Write internal node
            self.f_handle.seek(current_block_id * common_db.BLOCK_SIZE)
            self.f_handle.write(internal_node)
            current_block_id += 1
        
        self.num_of_levels += 1
        return self._create_internal_nodes(internal_nodes, current_block_id, field_type)

    # ------------------------------------------------
    # Create single internal node
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       child_nodes: list of child nodes for this internal node
    #       field_type: data type of the indexed field
    # Output:
    #       buffer containing internal node data
    # ------------------------------------------------
    def _create_internal_node(self, child_nodes, field_type):
        node = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
        struct.pack_into('!ii', node, 0, INTERNAL_NODE_TYPE, len(child_nodes))
        offset = 8
        
        for block_id, child_node_data in child_nodes:
            first_key = self._get_first_key_from_node(child_node_data, field_type)
            
            struct.pack_into('!8si', node, offset, first_key, block_id)
            offset += 12
        
        return node

    # ------------------------------------------------
    # Extract first key from a node
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       node_data: buffer containing node data
    #       field_type: data type of the indexed field
    # Output:
    #       first key in the node (8 bytes)
    # ------------------------------------------------
    def _get_first_key_from_node(self, node_data, field_type):
        node_type, num_keys = struct.unpack_from('!ii', node_data, 0)
        if num_keys > 0:
            first_key, _, _ = struct.unpack_from('!8sii', node_data, 8)
            return first_key
        return b'\x00' * 8

    # ------------------------------------------------
    # Create hash index from collected records
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       records: list of (key, block_id, offset) tuples
    #       field_type: data type of the indexed field
    # Output:
    #       boolean indicating success/failure
    # ------------------------------------------------
    def _create_hash_index(self, records, field_type):
        try:
            print(f"Creating hash index with {len(records)} records")
            
            # Initialize hash table
            hash_table = [[] for _ in range(HASH_TABLE_SIZE)]
            
            # Hash all records
            for key, block_id, offset in records:
                if field_type == 2:  # Integer
                    value_int = struct.unpack('!q', key)[0]
                    hash_value = hash(value_int) % HASH_TABLE_SIZE
                else:  # String
                    key_str = key.decode('utf-8', 'ignore').strip()
                    hash_value = hash(key_str) % HASH_TABLE_SIZE
                hash_table[hash_value].append((key, block_id, offset))
            
            # Write hash table to file
            self.f_handle.seek(common_db.BLOCK_SIZE)  # Skip header block
            bucket_ptrs = []
            current_pos = common_db.BLOCK_SIZE + HASH_TABLE_SIZE * 4
            
            # Write bucket pointer table
            for bucket in hash_table:
                if bucket:
                    bucket_ptrs.append(current_pos)
                    current_pos += len(bucket) * 16 + 4
                else:
                    bucket_ptrs.append(0)
            
            # Write bucket pointers
            for ptr in bucket_ptrs:
                self.f_handle.write(struct.pack('!i', ptr))
            
            # Write buckets
            for bucket in hash_table:
                if bucket:
                    self.f_handle.write(struct.pack('!i', len(bucket)))
                    for key, block_id, rec_offset in bucket:
                        self.f_handle.write(struct.pack('!8sii', key, block_id, rec_offset))
            
            # Update header
            self.has_root = True
            self.hash_table_size = HASH_TABLE_SIZE
            struct.pack_into('!i?iii', self.first_block_buf, 0, 1, True, 1, 1, HASH_INDEX)
            self.f_handle.seek(0)
            self.f_handle.write(self.first_block_buf)
            self.f_handle.flush()
            
            print(f"✓ Hash index created with {HASH_TABLE_SIZE} buckets")
            return True
        except Exception as e:
            print(f"✗ Error creating hash index: {str(e)}")
            return False

    # ------------------------------------------------
    # Search for records using index
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       field_name: name of the indexed field
    #       search_value: value to search for
    # Output:
    #       list of (block_id, offset) tuples for matching records
    # ------------------------------------------------
    def search_by_index(self, field_name, search_value):
        print(f"Searching using {'B-tree' if self.index_type == BTREE_INDEX else 'Hash'} index: {field_name} = {search_value}")
        
        if not self.has_root:
            print("No root node available")
            return []
        
        # 确保field_list存在
        if not hasattr(self, 'field_list') or not self.field_list:
            try:
                storage_obj = storage_db.Storage(self.table_name.encode('utf-8'), debug=False)
                self.field_list = storage_obj.getFieldList()
                del storage_obj
            except Exception as e:
                print(f"Error loading field list: {e}")
                return []
        
        # Get field type and index
        field_type = None
        field_index = None
        for idx, field in enumerate(self.field_list):
            field_name_in_table = field[0].decode('utf-8').strip() if isinstance(field[0], bytes) else str(field[0]).strip()
            if field_name_in_table == field_name:
                field_type = field[1]
                field_index = idx
                break
        
        if field_type is None:
            print(f"Field '{field_name}' not found")
            print(f"Available fields: {[f[0].decode('utf-8').strip() if isinstance(f[0], bytes) else str(f[0]).strip() for f in self.field_list]}")
            return []
        
        # Convert search value to proper format
        if field_type == 2:  # Integer
            try:
                search_int = int(search_value)
                search_key = struct.pack('!q', search_int)  # 8字节格式
            except ValueError:
                print(f"Invalid integer value: {search_value}")
                return []
        else:  # String
            if not hasattr(self, 'max_key_length'):
                self.max_key_length = 8
            search_key = search_value.ljust(self.max_key_length)[:self.max_key_length].encode('utf-8')
        
        if self.index_type == BTREE_INDEX:
            return self._btree_search(search_key, field_type)
        elif self.index_type == HASH_INDEX:
            return self._hash_search(search_key, field_type)
        else:
            return []

    def _hash_search(self, search_key, field_type):
        if field_type == 2:  # Integer
            search_int = struct.unpack('!q', search_key)[0]
            hash_value = hash(search_int) % self.hash_table_size
        else:  # String
            search_str = search_key.decode('utf-8', 'ignore').strip()
            hash_value = hash(search_str) % self.hash_table_size
        
        # Read bucket pointer
        self.f_handle.seek(common_db.BLOCK_SIZE + hash_value * 4)
        bucket_ptr_data = self.f_handle.read(4)
        if len(bucket_ptr_data) < 4:
            return []
        
        bucket_ptr = struct.unpack('!i', bucket_ptr_data)[0]
        if bucket_ptr == 0:
            return []
        
        # Read bucket
        self.f_handle.seek(bucket_ptr)
        bucket_size_data = self.f_handle.read(4)
        if len(bucket_size_data) < 4:
            return []
        
        bucket_size = struct.unpack('!i', bucket_size_data)[0]
        results = []
        
        bucket_data = self.f_handle.read(bucket_size * 16)
        
        for i in range(bucket_size):
            offset = i * 16
            if offset + 16 <= len(bucket_data):
                key, block_id, record_offset = struct.unpack_from('!8sii', bucket_data, offset)
                
                if self._keys_equal_simple(key, search_key, field_type):
                    results.append((block_id, record_offset))
        
        return results

    def _keys_equal_simple(self, key1, key2, field_type):
        try:
            if field_type == 2:  # Integer
                val1 = struct.unpack('!q', key1)[0]
                val2 = struct.unpack('!q', key2)[0]
                return val1 == val2
            else:  # String
                str1 = key1.decode('utf-8', 'ignore').strip('\x00').strip()
                str2 = key2.decode('utf-8', 'ignore').strip('\x00').strip()
                return str1 == str2
        except:
            return False

    def _btree_search(self, search_key, field_type):
        results = []
        
        try:
            first_leaf_block = self._find_first_leaf_with_key(search_key, field_type)
            
            if first_leaf_block == -1:
                return results
            
            current_leaf = first_leaf_block
            scanned_leaves = set() 
            while current_leaf != -1 and current_leaf not in scanned_leaves:
                scanned_leaves.add(current_leaf)
                
                node_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
                self.f_handle.seek(current_leaf * common_db.BLOCK_SIZE)
                self.f_handle.readinto(node_buf)
                
                node_type, num_keys = struct.unpack_from('!ii', node_buf, 0)
                
                if node_type != LEAF_NODE_TYPE:
                    break

                offset = 8
                found_in_this_leaf = False
                
                for i in range(num_keys):
                    key, block_id, record_offset = struct.unpack_from('!8sii', node_buf, offset)
                    
                    if self._keys_equal_simple(key, search_key, field_type):
                        results.append((block_id, record_offset))
                        found_in_this_leaf = True
                    
                    offset += 16
                
                if not found_in_this_leaf and len(results) > 0:
                    break
                current_leaf += 1
                
                if len(scanned_leaves) > 20:
                    break
        
        except Exception as e:
            print(f"B-tree search error: {e}")
        
        return results

    def _find_first_leaf_with_key(self, search_key, field_type):
        try:
            current_block = self.root_node_ptr
            
            while True:
                node_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
                self.f_handle.seek(current_block * common_db.BLOCK_SIZE)
                self.f_handle.readinto(node_buf)
                
                node_type, num_keys = struct.unpack_from('!ii', node_buf, 0)
                
                if node_type == LEAF_NODE_TYPE:
                    return current_block
                
                elif node_type == INTERNAL_NODE_TYPE:
                    offset = 8
                    next_child_ptr = -1
                    
                    for i in range(num_keys):
                        key, ptr = struct.unpack_from('!8si', node_buf, offset)
                        
                        if field_type == 2:
                            key_val = struct.unpack('!q', key)[0]
                            search_val = struct.unpack('!q', search_key)[0]
                            
                            if search_val <= key_val:
                                next_child_ptr = ptr
                                break
                        
                        offset += 12
                    
                    if next_child_ptr == -1:
                        if num_keys > 0:
                            offset = 8 + (num_keys - 1) * 12
                            _, next_child_ptr = struct.unpack_from('!8si', node_buf, offset)
                    
                    if next_child_ptr == -1:
                        return -1
                    
                    current_block = next_child_ptr
                else:
                    return -1
                    
        except Exception as e:
            print(f"Error finding first leaf: {e}")
            return -1

    # ------------------------------------------------
    # Compare if first key is less than second key
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       key1: first key to compare
    #       key2: second key to compare
    #       field_type: data type of the keys
    # Output:
    #       boolean indicating if key1 < key2
    # ------------------------------------------------
    def _key_less_than(self, key1, key2, field_type):
        try:
            if field_type == 2:  # Integer
                val1 = struct.unpack('!q', key1)[0]
                val2 = struct.unpack('!q', key2)[0]
                return val1 < val2
            else:  # String
                str1 = key1.decode('utf-8', 'ignore').strip('\x00').strip()
                str2 = key2.decode('utf-8', 'ignore').strip('\x00').strip()
                return str1 < str2
        except:
            return False

    # ------------------------------------------------
    # Destructor for Index class
    # Author: Xinjian Zhang 278254081@qq.com
    # Input:
    #       None
    # Output:
    #       None (closes file handles and cleanup)
    # ------------------------------------------------
    def __del__(self):
        if hasattr(self, 'f_handle') and self.f_handle:
            try:
                self.f_handle.close()
                print("Index file closed")
            except:
                pass