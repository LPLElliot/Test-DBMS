# -----------------------
# index_db.py
# modified by: WuShuCheng  2396878284@qq.com
# modified by: Xinjian Zhang 278254081@qq.com
# -----------------------------------
# Index management module
# Implements B-tree index creation, deletion and search functionality
# ---------------------------------------

import os
import struct
import ctypes
import traceback
import common_db
import storage_db

# constant definitions
BTREE_INDEX = 1        # B-tree index type
HASH_INDEX = 2         # hash index type 
LEAF_NODE_TYPE = 0     # leaf node
INTERNAL_NODE_TYPE = 1 # internal node

# B-tree configuration
BTREE_ORDER = 64       # B-tree order (number of keys per node)
HASH_TABLE_SIZE = 1024 # Hash table size

# ----------------------------------------------
# author: Xinjian Zhang 278254081@qq.com
# Index class
# Functionality:
#   - Initialize index object
#   - Create B-tree/hash index
#   - Index search
#   - Delete index
# ----------------------------------------------
class Index(object):
    
    def __init__(self, tablename, index_type=BTREE_INDEX):
        print(f"Initializing {['', 'B-tree', 'Hash'][index_type]} index for table: {tablename}")
        self.table_name = tablename.strip()
        self.index_type = index_type
        self.has_root = False
        self.num_of_levels = 0
        self.root_node_ptr = -1
        self.current_block_id = 1
        self.hash_table_size = HASH_TABLE_SIZE if index_type == HASH_INDEX else 0
        
        try:
            self.first_block_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            index_filename = tablename + ('.hash' if index_type == HASH_INDEX else '.ind')
            
            if not os.path.exists(index_filename):
                print(f'Creating new index file: {index_filename}')
                self.f_handle = open(index_filename, 'wb+')
                # Initialize index file header
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
            raise

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
            
            print(f"Debug: Looking for field '{target_field}'")
            for i, field in enumerate(field_list):
                field_name_in_list = field[0].decode('utf-8').strip() if isinstance(field[0], bytes) else str(field[0]).strip()
                print(f"Debug: Checking field {i}: '{field_name_in_list}' (type: {field[1]})")
                if field_name_in_list == target_field:
                    field_index = i
                    field_type = field[1]
                    print(f"Debug: Found matching field at index {i}")
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

    def _collect_records(self, storage_obj, field_list, field_index, field_type):
        records = []
        storage_obj.f_handle.seek(0)
        header_block = storage_obj.f_handle.read(common_db.BLOCK_SIZE)
        if len(header_block) < 12:
            print("Error: Data file header too small")
            return []
        block_id, data_block_num, num_of_fields = struct.unpack('!iii', header_block[:12])
            
        if data_block_num == 0:
            print("No data blociks found")
            return []
            
        for block_id in range(1, data_block_num + 1):
            storage_obj.f_handle.seek(block_id * common_db.BLOCK_SIZE)
            block = storage_obj.f_handle.read(common_db.BLOCK_SIZE)
            if len(block) < 8:
                continue
                    
            block_header, num_records = struct.unpack('!ii', block[:8])
                
            if num_records == 0:
                continue
                
            offsets = []
            for i in range(num_records):
                offset_pos = 8 + i * 4
                if offset_pos + 4 <= len(block):
                    offset = struct.unpack('!i', block[offset_pos:offset_pos + 4])[0]
                    offsets.append(offset)
            
            for rec_idx, start_offset in enumerate(offsets):
                if start_offset + 18 > len(block):
                    continue
                        
                record_data_start = start_offset + 18
                    
                curr_offset = record_data_start
                record = []
                    
                for field_idx, field in enumerate(field_list):
                    field_len = field[2]
                    if curr_offset + field_len > len(block):
                        break
                    field_data = block[curr_offset:curr_offset + field_len]
                    record.append(field_data)
                    curr_offset += field_len
                
            if len(record) <= field_index:
                continue
                    
            key = record[field_index]
                
            if field_type == 2:  # Integer type
                if len(key) >= 4:
                    try:
                        value_int = struct.unpack('!i', key[:4])[0]
                        key = struct.pack('!q', value_int)
                    except Exception as e:
                        continue
                else:
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

    def _create_btree_index(self, records, field_type):
        """Create optimized B-tree index"""
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

    def _create_leaf_node(self, records, block_id):
        node = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
        struct.pack_into('!ii', node, 0, LEAF_NODE_TYPE, len(records))
        
        offset = 8
        for key, data_block_id, rec_offset in records:
            struct.pack_into('!8sii', node, offset, key, data_block_id, rec_offset)
            offset += 16
            
        return node

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

    def _create_internal_node(self, child_nodes, field_type):
        node = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
        struct.pack_into('!ii', node, 0, INTERNAL_NODE_TYPE, len(child_nodes))
        
        offset = 8
        for block_id, child_node in child_nodes:
            # Get the first key from child node
            first_key = self._get_first_key_from_node(child_node, field_type)
            struct.pack_into('!8si', node, offset, first_key, block_id)
            offset += 12
            
        return node

    def _get_first_key_from_node(self, node_data, field_type):
        node_type, num_keys = struct.unpack_from('!ii', node_data, 0)
        if num_keys > 0:
            first_key, _, _ = struct.unpack_from('!8sii', node_data, 8)
            return first_key
        return b'\x00' * 8

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

    def search_by_index(self, field_name, search_value):
        try:
            print(f"Searching using {['', 'B-tree', 'Hash'][self.index_type]} index: {field_name} = {search_value}")
            
            if not self.has_root:
                print("Index is empty")
                return []
            
            # Get field type
            storage_obj = storage_db.Storage(self.table_name.encode('utf-8'))
            field_list = storage_obj.getFieldList()
            field_type = None
            target_field = field_name.encode('utf-8') if isinstance(field_name, str) else field_name
            
            for field in field_list:
                if field[0].strip() == target_field.strip():
                    field_type = field[1]
                    break
                    
            if field_type is None:
                print(f"Field not found: {field_name}")
                return []
            
            # Prepare search key
            if field_type == 2:  # Integer
                try:
                    search_int = int(search_value)
                    search_key = struct.pack('!q', search_int)
                except ValueError:
                    print(f"Invalid integer value: {search_value}")
                    return []
            else:  # String
                search_key = search_value.encode('utf-8')[:8].ljust(8, b'\x00')
            
            # Search based on index type
            if self.index_type == HASH_INDEX:
                return self._hash_search(search_key, field_type)
            else:
                return self._btree_search(search_key, field_type)
                
        except Exception as e:
            print(f"Index search error: {str(e)}")
            traceback.print_exc()
            return []

    def _btree_search(self, search_key, field_type):
        results = []
        current_block = self.root_node_ptr
        
        # Traverse down to leaf
        for level in range(self.num_of_levels):
            node_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            self.f_handle.seek(current_block * common_db.BLOCK_SIZE)
            self.f_handle.readinto(node_buf)
            
            node_type, num_keys = struct.unpack_from('!ii', node_buf, 0)
            
            if node_type == LEAF_NODE_TYPE:
                # Search in leaf node
                offset = 8
                for i in range(num_keys):
                    key, block_id, record_offset = struct.unpack_from('!8sii', node_buf, offset)
                    
                    if self._keys_equal(key, search_key, field_type):
                        if field_type == 2:
                            key_val = struct.unpack('!q', key)[0]
                            print(f"Found matching record (integer): {key_val} -> block{block_id}:offset{record_offset}")
                        else:
                            key_str = key.decode('utf-8', 'ignore').strip()
                            print(f"Found matching record (string): '{key_str}' -> block{block_id}:offset{record_offset}")
                        results.append((block_id, record_offset))
                    
                    offset += 16
                break
            else:
                # Internal node - find next block to search
                found_next = False
                offset = 8
                for i in range(num_keys):
                    key, child_block = struct.unpack_from('!8si', node_buf, offset)
                    if self._key_less_than(search_key, key, field_type):
                        current_block = child_block
                        found_next = True
                        break
                    offset += 12
                
                if not found_next:
                    # Use last child
                    _, last_child = struct.unpack_from('!8si', node_buf, offset - 12)
                    current_block = last_child
        
        print(f"B-tree search found {len(results)} matching records")
        return results

    def _hash_search(self, search_key, field_type):
        # Calculate hash value
        if field_type == 2:  # Integer
            search_int = struct.unpack('!q', search_key)[0]
            hash_value = hash(search_int) % self.hash_table_size
        else:  # String
            search_str = search_key.decode('utf-8', 'ignore').strip()
            hash_value = hash(search_str) % self.hash_table_size
        
        print(f"Hash value: {hash_value}")
        
        # Read bucket pointer
        self.f_handle.seek(common_db.BLOCK_SIZE + hash_value * 4)
        bucket_ptr_data = self.f_handle.read(4)
        if len(bucket_ptr_data) < 4:
            return []
        
        bucket_ptr = struct.unpack('!i', bucket_ptr_data)[0]
        if bucket_ptr == 0:
            print("Empty bucket")
            return []
        
        # Read bucket
        self.f_handle.seek(bucket_ptr)
        bucket_size_data = self.f_handle.read(4)
        if len(bucket_size_data) < 4:
            return []
        
        bucket_size = struct.unpack('!i', bucket_size_data)[0]
        results = []
        
        for i in range(bucket_size):
            record_data = self.f_handle.read(16)
            if len(record_data) < 16:
                break
            
            key, block_id, record_offset = struct.unpack('!8sii', record_data)
            
            if self._keys_equal(key, search_key, field_type):
                if field_type == 2:
                    key_val = struct.unpack('!q', key)[0]
                    print(f"Found matching record (integer): {key_val} -> block{block_id}:offset{record_offset}")
                else:
                    key_str = key.decode('utf-8', 'ignore').strip()
                    print(f"Found matching record (string): '{key_str}' -> block{block_id}:offset{record_offset}")
                results.append((block_id, record_offset))
        
        print(f"Hash search found {len(results)} matching records")
        return results

    def _keys_equal(self, key1, key2, field_type):
        if field_type == 2:  # Integer
            try:
                val1 = struct.unpack('!q', key1)[0]
                val2 = struct.unpack('!q', key2)[0]
                return val1 == val2
            except:
                return False
        else:  # String
            return key1.strip(b'\x00') == key2.strip(b'\x00')

    def _key_less_than(self, key1, key2, field_type):
        if field_type == 2:  # Integer
            try:
                val1 = struct.unpack('!q', key1)[0]
                val2 = struct.unpack('!q', key2)[0]
                return val1 < val2
            except:
                return False
        else:  # String
            return key1.strip(b'\x00') < key2.strip(b'\x00')

    def __del__(self):
        if hasattr(self, 'f_handle') and self.f_handle:
            try:
                self.f_handle.close()
                print("Index file closed")
            except:
                pass