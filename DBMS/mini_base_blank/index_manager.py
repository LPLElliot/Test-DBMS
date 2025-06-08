# -----------------------
# index_manager.py
# author: WuShuCheng  2396878284@qq.com
# modified by: Xinjian Zhang 278254081@qq.com
# -----------------------------------
# Index management interface module
# Provides user interface for index creation, deletion, performance testing
# ---------------------------------------

import os
import time
import storage_db
from index_db import Index, BTREE_INDEX, HASH_INDEX

def handle_index_management(schemaObj):
    index_menu = '''
 +-----------------------------------------+
 |           INDEX MANAGEMENT              |
 +-----------------------------------------+
 | 1: Create B-tree index                  |
 | 2: Create Hash index                    |
 | 3: Drop index                           |
 | 4: Test index performance               |
 | 5: Compare index types                  |
 | 6: List all indexes                     |
 | 7: Back to main menu                    |
 +-----------------------------------------+
 Your choice: '''
    
    while True:
        index_choice = input(index_menu)
        if index_choice == '1':
            create_index(schemaObj, BTREE_INDEX)
        elif index_choice == '2':
            create_index(schemaObj, HASH_INDEX)
        elif index_choice == '3':
            drop_index(schemaObj)
        elif index_choice == '4':
            test_index_performance(schemaObj)
        elif index_choice == '5':
            compare_index_types(schemaObj)
        elif index_choice == '6':
            list_all_indexes(schemaObj)
        elif index_choice == '7':
            break
        else:
            print("Invalid choice! Please try again.")

def create_index(schemaObj, index_type=BTREE_INDEX):
    index_type_name = "B-tree" if index_type == BTREE_INDEX else "Hash"
    print(f"=== CREATE {index_type_name.upper()} INDEX ===")
    
    print("Available tables:")
    table_list = list(schemaObj.get_table_name_list())
    if not table_list:
        print("No tables found.")
        return
        
    for i, table in enumerate(table_list, 1):
        table_display = table.decode('utf-8') if isinstance(table, bytes) else str(table)
        print(f"{i}. {table_display.strip()}")
    
    table_name = input('Enter table name: ').strip()
    if isinstance(table_name, str):
        table_name = table_name.encode('utf-8')
    
    if table_name.strip() in schemaObj.get_table_name_list():
        # Display available fields
        dataObj = storage_db.Storage(table_name, debug=False)  # 不显示调试信息
        field_list = dataObj.getFieldList()
        print("Available fields:")
        for i, field in enumerate(field_list, 1):
            field_name = field[0].decode('utf-8') if isinstance(field[0], bytes) else str(field[0])
            field_type = {0: "STRING", 1: "VARSTRING", 2: "INTEGER", 3: "BOOLEAN"}.get(field[1], "UNKNOWN")
            print(f"{i}. {field_name.strip()} ({field_type})")
        
        field_name = input('Enter field name for indexing: ').strip()
        
        # Check if field exists
        field_exists = False
        for field in field_list:
            field_name_in_table = field[0].decode('utf-8') if isinstance(field[0], bytes) else str(field[0])
            if field_name_in_table.strip() == field_name:
                field_exists = True
                break
        
        if field_exists:
            try:
                print(f"Creating {index_type_name} index...")
                start_time = time.time()
                
                # Create index
                idx = Index(table_name.decode('utf-8'), index_type)
                success = idx.create_index(field_name, index_type)
                
                creation_time = time.time() - start_time
                
                if success:
                    print(f"✓ {index_type_name} index created successfully!")
                    print(f"  Creation time: {creation_time:.4f} seconds")
                    
                    # Get index file info
                    index_ext = '.hash' if index_type == HASH_INDEX else '.ind'
                    index_file = f"{table_name.decode('utf-8')}{index_ext}"
                    if os.path.exists(index_file):
                        file_size = os.path.getsize(index_file)
                        print(f"  Index file: {index_file}")
                        print(f"  File size: {file_size} bytes")
                else:
                    print(f"✗ Failed to create {index_type_name} index")
                del idx
            except Exception as e:
                print(f"Error creating index: {e}")
        else:
            print(f"Field '{field_name}' not found in table")
        del dataObj
    else:
        print("Table not found!")

def drop_index(schemaObj):
    print("=== DROP INDEX ===")
    print("Available tables:")
    table_list = list(schemaObj.get_table_name_list())
    for i, table in enumerate(table_list, 1):
        table_display = table.decode('utf-8') if isinstance(table, bytes) else str(table)
        print(f"{i}. {table_display.strip()}")
    
    table_name = input('Enter table name: ').strip()
    
    # Check for existing index files
    btree_file = f"{table_name}.ind"
    hash_file = f"{table_name}.hash"
    
    existing_indexes = []
    if os.path.exists(btree_file):
        existing_indexes.append(("B-tree", btree_file))
    if os.path.exists(hash_file):
        existing_indexes.append(("Hash", hash_file))
    
    if not existing_indexes:
        print("No indexes found for this table.")
        return
    
    print("Existing indexes:")
    for i, (index_type, filename) in enumerate(existing_indexes, 1):
        file_size = os.path.getsize(filename)
        print(f"{i}. {index_type} index ({filename}) - {file_size} bytes")
    
    if len(existing_indexes) == 1:
        choice = '1'
    else:
        choice = input("Choose index to drop (number): ").strip()
    
    try:
        index_num = int(choice) - 1
        if 0 <= index_num < len(existing_indexes):
            index_type, filename = existing_indexes[index_num]
            confirm = input(f"Are you sure you want to drop the {index_type} index? (y/n): ").strip().lower()
            if confirm == 'y':
                os.remove(filename)
                print(f"✓ {index_type} index dropped successfully!")
            else:
                print("Operation cancelled.")
        else:
            print("Invalid choice.")
    except (ValueError, FileNotFoundError) as e:
        print(f"Error dropping index: {e}")

def test_index_performance(schemaObj):
    print("=== INDEX PERFORMANCE TEST ===")
    try:
        table_name = input('Enter table name: ').strip()
        if isinstance(table_name, str):
            table_name = table_name.encode('utf-8')
        
        if table_name.strip() not in schemaObj.get_table_name_list():
            print("Table not found!")
            return
        
        # Display available fields
        dataObj = storage_db.Storage(table_name, debug=False)
        field_list = dataObj.getFieldList()
        print("Available fields:")
        for i, field in enumerate(field_list, 1):
            field_name = field[0].decode('utf-8') if isinstance(field[0], bytes) else str(field[0])
            field_type = {0: "STRING", 1: "VARSTRING", 2: "INTEGER", 3: "BOOLEAN"}.get(field[1], "UNKNOWN")
            print(f"{i}. {field_name.strip()} ({field_type})")
        
        field_name = input('Enter field name: ').strip()
        search_value = input('Enter search value: ').strip()
        
        print(f"\n{'='*70}")
        print("PERFORMANCE COMPARISON TEST")
        print(f"{'='*70}")
        print(f"Table: {table_name.decode('utf-8')}")
        print(f"Field: {field_name}")
        print(f"Search Value: {search_value}")
        print(f"{'='*70}")
        
        # 1. Sequential scan test
        print("\n[1] Sequential Scan Test")
        print("-" * 50)
        start_time = time.time()
        results = dataObj.find_record_by_field(field_name, search_value)
        sequential_time = time.time() - start_time
        print(f"Records found: {len(results)}")
        print(f"Time elapsed: {sequential_time:.6f} seconds")
        
        # 2. B-tree index test
        btree_time = None
        btree_results = []
        btree_file = f"{table_name.decode('utf-8')}.ind"
        if os.path.exists(btree_file):
            print(f"\n[2] B-tree Index Search Test")
            print("-" * 50)
            start_time = time.time()
            idx = Index(table_name.decode('utf-8'), BTREE_INDEX)
            btree_results = idx.search_by_index(field_name, search_value)
            btree_time = time.time() - start_time
            print(f"Records found: {len(btree_results)}")
            print(f"Time elapsed: {btree_time:.6f} seconds")
            del idx
        else:
            print(f"\n[2] B-tree Index: Not found")
        
        # 3. Hash index test
        hash_time = None
        hash_results = []
        hash_file = f"{table_name.decode('utf-8')}.hash"
        if os.path.exists(hash_file):
            print(f"\n[3] Hash Index Search Test")
            print("-" * 50)
            start_time = time.time()
            idx = Index(table_name.decode('utf-8'), HASH_INDEX)
            hash_results = idx.search_by_index(field_name, search_value)
            hash_time = time.time() - start_time
            print(f"Records found: {len(hash_results)}")
            print(f"Time elapsed: {hash_time:.6f} seconds")
            del idx
        else:
            print(f"\n[3] Hash Index: Not found")
        
        # 4. Performance analysis
        print(f"\n[4] Performance Analysis")
        print("-" * 50)
        print(f"Sequential scan time:     {sequential_time:.6f} seconds")
        
        if btree_time is not None:
            if btree_time > 0:
                btree_speedup = sequential_time / btree_time
                print(f"B-tree index time:        {btree_time:.6f} seconds (speedup: {btree_speedup:.2f}x)")
            else:
                print(f"B-tree index time:        {btree_time:.6f} seconds (speedup: ∞x)")
        
        if hash_time is not None:
            if hash_time > 0:
                hash_speedup = sequential_time / hash_time
                print(f"Hash index time:          {hash_time:.6f} seconds (speedup: {hash_speedup:.2f}x)")
            else:
                print(f"Hash index time:          {hash_time:.6f} seconds (speedup: ∞x)")
        
        # Compare index performance (避免除零错误)
        if btree_time is not None and hash_time is not None:
            if btree_time > 0 and hash_time > 0:
                if btree_time < hash_time:
                    performance_winner = "B-tree"
                    performance_ratio = hash_time / btree_time
                elif hash_time < btree_time:
                    performance_winner = "Hash"
                    performance_ratio = btree_time / hash_time
                else:
                    performance_winner = "Tie"
                    performance_ratio = 1.0
                
                print(f"\nBest performing index:    {performance_winner}")
                if performance_winner != "Tie":
                    print(f"Performance advantage:    {performance_ratio:.2f}x faster")
            else:
                if btree_time == 0 and hash_time == 0:
                    print(f"\nBoth indexes performed instantly")
                elif btree_time == 0:
                    print(f"\nBest performing index:    B-tree (instant)")
                else:
                    print(f"\nBest performing index:    Hash (instant)")
        
        # Result consistency check
        all_consistent = True
        baseline_count = len(results)
        
        if btree_results and len(btree_results) != baseline_count:
            all_consistent = False
        if hash_results and len(hash_results) != baseline_count:
            all_consistent = False
        
        consistency_status = "✓ Consistent" if all_consistent else "✗ Inconsistent"
        print(f"Result consistency:       {consistency_status}")
        
        del dataObj
        print(f"\n{'='*70}")
        
    except Exception as e:
        print(f"Error during performance test: {e}")
        import traceback
        traceback.print_exc()

def compare_index_types(schemaObj):
    """Compare different index types on the same data"""
    print("=== INDEX TYPE COMPARISON ===")
    
    table_name = input('Enter table name: ').strip()
    if isinstance(table_name, str):
        table_name = table_name.encode('utf-8')
    
    if table_name.strip() not in schemaObj.get_table_name_list():
        print("Table not found!")
        return
    
    dataObj = storage_db.Storage(table_name, debug=False)  # 不显示调试信息
    field_list = dataObj.getFieldList()
    print("Available fields:")
    for i, field in enumerate(field_list, 1):
        field_name = field[0].decode('utf-8') if isinstance(field[0], bytes) else str(field[0])
        field_type = {0: "STRING", 1: "VARSTRING", 2: "INTEGER", 3: "BOOLEAN"}.get(field[1], "UNKNOWN")
        print(f"{i}. {field_name.strip()} ({field_type})")
    
    field_name = input('Enter field name for comparison: ').strip()
    
    print(f"\n{'='*80}")
    print("INDEX TYPE COMPARISON")
    print(f"{'='*80}")
    print(f"Table: {table_name.decode('utf-8')}")
    print(f"Field: {field_name}")
    print(f"{'='*80}")
    
    # Create both index types
    print("\n[1] Creating B-tree Index...")
    print("-" * 50)
    start_time = time.time()
    btree_idx = Index(table_name.decode('utf-8'), BTREE_INDEX)
    btree_success = btree_idx.create_index(field_name, BTREE_INDEX)
    btree_creation_time = time.time() - start_time
    
    print(f"B-tree creation time: {btree_creation_time:.4f} seconds")
    print(f"B-tree creation: {'✓ Success' if btree_success else '✗ Failed'}")
    
    print("\n[2] Creating Hash Index...")
    print("-" * 50)
    start_time = time.time()
    hash_idx = Index(table_name.decode('utf-8'), HASH_INDEX)
    hash_success = hash_idx.create_index(field_name, HASH_INDEX)
    hash_creation_time = time.time() - start_time
    
    print(f"Hash creation time: {hash_creation_time:.4f} seconds")
    print(f"Hash creation: {'✓ Success' if hash_success else '✗ Failed'}")
    
    if btree_success and hash_success:
        # Test multiple search values
        test_values = []
        print("\nEnter test values (press Enter with empty value to finish):")
        while True:
            value = input("Search value: ").strip()
            if not value:
                break
            test_values.append(value)
        
        if not test_values:
            test_values = ["test1", "test2", "test3"]  # Default values
        
        print(f"\n[3] Performance Comparison ({len(test_values)} searches)")
        print("-" * 50)
        
        total_btree_time = 0
        total_hash_time = 0
        
        for i, search_value in enumerate(test_values, 1):
            print(f"\nSearch {i}: '{search_value}'")
            
            # B-tree search
            start_time = time.time()
            btree_results = btree_idx.search_by_index(field_name, search_value)
            btree_search_time = time.time() - start_time
            total_btree_time += btree_search_time
            
            # Hash search
            start_time = time.time()
            hash_results = hash_idx.search_by_index(field_name, search_value)
            hash_search_time = time.time() - start_time
            total_hash_time += hash_search_time
            
            print(f"  B-tree: {len(btree_results)} results in {btree_search_time:.6f}s")
            print(f"  Hash:   {len(hash_results)} results in {hash_search_time:.6f}s")
            
            if len(btree_results) != len(hash_results):
                print(f"  ⚠ Warning: Result count mismatch!")
        
        print(f"\n[4] Summary")
        print("-" * 50)
        print(f"B-tree creation time:     {btree_creation_time:.4f} seconds")
        print(f"Hash creation time:       {hash_creation_time:.4f} seconds")
        print(f"B-tree total search time: {total_btree_time:.6f} seconds")
        print(f"Hash total search time:   {total_hash_time:.6f} seconds")
        print(f"B-tree avg search time:   {total_btree_time/len(test_values):.6f} seconds")
        print(f"Hash avg search time:     {total_hash_time/len(test_values):.6f} seconds")
        
        # File size comparison
        btree_file = f"{table_name.decode('utf-8')}.ind"
        hash_file = f"{table_name.decode('utf-8')}.hash"
        
        if os.path.exists(btree_file) and os.path.exists(hash_file):
            btree_size = os.path.getsize(btree_file)
            hash_size = os.path.getsize(hash_file)
            print(f"B-tree index size:        {btree_size} bytes")
            print(f"Hash index size:          {hash_size} bytes")
            
            if total_btree_time < total_hash_time:
                print(f"Winner: B-tree (faster by {total_hash_time/total_btree_time:.2f}x)")
            elif total_hash_time < total_btree_time:
                print(f"Winner: Hash (faster by {total_btree_time/total_hash_time:.2f}x)")
            else:
                print("Result: Tie in performance")
    
    # Cleanup
    try:
        del dataObj
    except:
        pass
    
    print(f"\n{'='*80}")

def list_all_indexes(schemaObj):
    print("=== LIST ALL INDEXES ===")
    found_indexes = []
    
    for table_name in schemaObj.get_table_name_list():
        table_str = table_name.decode('utf-8') if isinstance(table_name, bytes) else str(table_name)
        
        # Check for B-tree index
        btree_file = f"{table_str}.ind"
        if os.path.exists(btree_file):
            file_size = os.path.getsize(btree_file)
            found_indexes.append((table_str, "B-tree", btree_file, file_size))
        
        # Check for Hash index
        hash_file = f"{table_str}.hash"
        if os.path.exists(hash_file):
            file_size = os.path.getsize(hash_file)
            found_indexes.append((table_str, "Hash", hash_file, file_size))
    
    if found_indexes:
        print("Existing indexes:")
        print("-" * 70)
        print(f"{'Table':<15} {'Type':<10} {'File':<20} {'Size':<10} {'Status'}")
        print("-" * 70)
        for table_name, index_type, index_file, file_size in found_indexes:
            print(f"{table_name:<15} {index_type:<10} {index_file:<20} {file_size:<10} Active")
        print("-" * 70)
        print(f"Total indexes: {len(found_indexes)}")
    else:
        print("No indexes found.")