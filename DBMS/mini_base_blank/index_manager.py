# ------------------------------------------------
# index_manager.py
# Author: WuShuCheng  2396878284@qq.com
# Modified by: Xinjian Zhang 278254081@qq.com
# ------------------------------------------------
# Simplified index management interface module
# Provides 4 core functions: compare, delete all, list, exit
# ------------------------------------------------

import os
import time
import storage_db
from index_db import Index, BTREE_INDEX, HASH_INDEX

# ------------------------------------------------
# Simplified index management menu handler
# Author: WuShuCheng  2396878284@qq.com
# Input:
#       schemaObj: Schema object containing table metadata
# Output:
#       None (interactive menu interface)
# ------------------------------------------------
def handle_index_management(schemaObj):
    index_menu = '''
 +-----------------------------------------+
 |           INDEX MANAGEMENT              |
 +-----------------------------------------+
 | 1: Index Performance Comparison         |
 | 2: Delete All Indexes                   |
 | 3: List All Indexes                     |
 | 4: Back to Main Menu                    |
 +-----------------------------------------+
 Your choice: '''
    
    while True:
        index_choice = input(index_menu)
        if index_choice == '1':
            comprehensive_index_comparison(schemaObj)
        elif index_choice == '2':
            delete_all_indexes(schemaObj)
        elif index_choice == '3':
            list_all_indexes(schemaObj)
        elif index_choice == '4':
            break
        else:
            print("Invalid choice! Please try again.")

# ------------------------------------------------
# Comprehensive index performance comparison
# Creates both index types and compares with sequential scan
# Author: WuShuCheng  2396878284@qq.com
# Input:
#       schemaObj: Schema object containing table metadata
# Output:
#       None (displays comprehensive performance comparison)
# ------------------------------------------------
def comprehensive_index_comparison(schemaObj):
    print("=== COMPREHENSIVE INDEX PERFORMANCE COMPARISON ===")
    
    # Select table
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
    
    if table_name.strip() not in schemaObj.get_table_name_list():
        print("Table not found!")
        return
    
    # Select field
    dataObj = storage_db.Storage(table_name, debug=False)
    field_list = dataObj.getFieldList()
    print("\nAvailable fields:")
    for i, field in enumerate(field_list, 1):
        field_name = field[0].decode('utf-8') if isinstance(field[0], bytes) else str(field[0])
        field_type = {0: "STRING", 1: "VARSTRING", 2: "INTEGER", 3: "BOOLEAN"}.get(field[1], "UNKNOWN")
        print(f"{i}. {field_name.strip()} ({field_type})")
    
    field_name = input('Enter field name for comparison: ').strip()
    
    # Get search values
    test_values = []
    print("\nEnter test values (press Enter with empty value to finish):")
    while True:
        value = input("Search value: ").strip()
        if not value:
            break
        test_values.append(value)
    
    if not test_values:
        # Auto-suggest values based on field type
        field_type = None
        for field in field_list:
            if field[0].decode('utf-8').strip() == field_name:
                field_type = field[1]
                break
        
        if field_type == 2:  # Integer
            test_values = ["1", "10", "100"]
            print(f"Using default integer test values: {test_values}")
        else:  # String
            test_values = ["Student001", "Student050", "Student100"]
            print(f"Using default string test values: {test_values}")
    
    print(f"\n{'='*80}")
    print("COMPREHENSIVE PERFORMANCE COMPARISON")
    print(f"{'='*80}")
    print(f"Table: {table_name.decode('utf-8')}")
    print(f"Field: {field_name}")
    print(f"Test values: {test_values}")
    print(f"{'='*80}")
    
    # Phase 1: Create indexes
    print("\n[PHASE 1] Creating Indexes")
    print("-" * 50)
    
    # Create B-tree index
    print("Creating B-tree index...")
    start_time = time.perf_counter()
    btree_idx = Index(table_name.decode('utf-8'), BTREE_INDEX)
    btree_success = btree_idx.create_index(field_name, BTREE_INDEX)
    btree_creation_time = time.perf_counter() - start_time
    print(f"B-tree creation: {'✓ Success' if btree_success else '✗ Failed'} ({btree_creation_time:.4f}s)")
    
    # Create Hash index
    print("Creating Hash index...")
    start_time = time.perf_counter()
    hash_idx = Index(table_name.decode('utf-8'), HASH_INDEX)
    hash_success = hash_idx.create_index(field_name, HASH_INDEX)
    hash_creation_time = time.perf_counter() - start_time
    print(f"Hash creation: {'✓ Success' if hash_success else '✗ Failed'} ({hash_creation_time:.4f}s)")
    
    if not (btree_success and hash_success):
        print("Index creation failed. Cannot proceed with comparison.")
        return
    
    # Phase 2: Performance comparison
    print(f"\n[PHASE 2] Performance Testing ({len(test_values)} searches)")
    print("-" * 50)
    
    total_sequential_time = 0
    total_btree_time = 0
    total_hash_time = 0
    all_results_consistent = True
    
    for i, search_value in enumerate(test_values, 1):
        print(f"\nTest {i}: Searching for '{search_value}'")
        
        # Sequential scan
        start_time = time.perf_counter()
        sequential_results = dataObj.find_record_by_field(field_name, search_value)
        sequential_time = time.perf_counter() - start_time
        total_sequential_time += sequential_time
        
        # B-tree search
        start_time = time.perf_counter()
        btree_results = btree_idx.search_by_index(field_name, search_value)
        btree_time = time.perf_counter() - start_time
        total_btree_time += btree_time
        
        # Hash search
        start_time = time.perf_counter()
        hash_results = hash_idx.search_by_index(field_name, search_value)
        hash_time = time.perf_counter() - start_time
        total_hash_time += hash_time
        
        # Display results
        print(f"  Sequential Scan: {len(sequential_results)} records in {sequential_time:.6f}s")
        print(f"  B-tree Index:    {len(btree_results)} records in {btree_time:.6f}s")
        print(f"  Hash Index:      {len(hash_results)} records in {hash_time:.6f}s")
        
        # Check consistency
        if len(sequential_results) != len(btree_results) or len(sequential_results) != len(hash_results):
            print(f"  ⚠️ Warning: Result count mismatch!")
            all_results_consistent = False
    
    # Phase 3: Summary and analysis
    print(f"\n[PHASE 3] Performance Summary")
    print("-" * 50)
    
    print(f"Index Creation Times:")
    print(f"  B-tree: {btree_creation_time:.4f} seconds")
    print(f"  Hash:   {hash_creation_time:.4f} seconds")
    
    print(f"\nTotal Search Times ({len(test_values)} searches):")
    print(f"  Sequential Scan: {total_sequential_time:.6f} seconds")
    print(f"  B-tree Index:    {total_btree_time:.6f} seconds")
    print(f"  Hash Index:      {total_hash_time:.6f} seconds")
    
    print(f"\nAverage Search Times:")
    avg_sequential = total_sequential_time / len(test_values)
    avg_btree = total_btree_time / len(test_values)
    avg_hash = total_hash_time / len(test_values)
    print(f"  Sequential Scan: {avg_sequential:.6f} seconds")
    print(f"  B-tree Index:    {avg_btree:.6f} seconds")
    print(f"  Hash Index:      {avg_hash:.6f} seconds")
    
    # Performance analysis
    print(f"\n[PHASE 4] Performance Analysis")
    print("-" * 50)
    
    # Calculate speedups
    if total_btree_time > 0:
        btree_speedup = total_sequential_time / total_btree_time
        print(f"B-tree speedup vs Sequential: {btree_speedup:.2f}x")
    else:
        print(f"B-tree speedup vs Sequential: ∞x (instant)")
    
    if total_hash_time > 0:
        hash_speedup = total_sequential_time / total_hash_time
        print(f"Hash speedup vs Sequential:   {hash_speedup:.2f}x")
    else:
        print(f"Hash speedup vs Sequential:   ∞x (instant)")
    
    # Winner determination
    times = [
        ("Sequential Scan", total_sequential_time),
        ("B-tree Index", total_btree_time),
        ("Hash Index", total_hash_time)
    ]
    times.sort(key=lambda x: x[1])
    winner = times[0]
    
    print(f"\n🏆 Performance Winner: {winner[0]} ({winner[1]:.6f}s total)")
    
    if winner[0] == "Sequential Scan":
        print("   Note: Sequential scan won due to small data size or overhead.")
        print("   Try with larger datasets to see index advantages.")
    
    # File size comparison
    btree_file = f"{table_name.decode('utf-8')}.ind"
    hash_file = f"{table_name.decode('utf-8')}.hash"
    if os.path.exists(btree_file) and os.path.exists(hash_file):
        btree_size = os.path.getsize(btree_file)
        hash_size = os.path.getsize(hash_file)
        print(f"\nStorage Comparison:")
        print(f"  B-tree index: {btree_size} bytes")
        print(f"  Hash index:   {hash_size} bytes")
        if btree_size < hash_size:
            print(f"  B-tree is more compact by {hash_size - btree_size} bytes")
        elif hash_size < btree_size:
            print(f"  Hash is more compact by {btree_size - hash_size} bytes")
        else:
            print(f"  Both indexes have same size")
    
    # Consistency check
    consistency_status = "✅ All results consistent" if all_results_consistent else "❌ Result inconsistencies detected"
    print(f"\nResult Consistency: {consistency_status}")
    
    # Cleanup
    try:
        del dataObj
        del btree_idx
        del hash_idx
    except:
        pass
    
    print(f"\n{'='*80}")

# ------------------------------------------------
# Delete all index files
# Author: WuShuCheng  2396878284@qq.com
# Input:
#       schemaObj: Schema object containing table metadata
# Output:
#       None (removes all index files)
# ------------------------------------------------
def delete_all_indexes(schemaObj):
    print("=== DELETE ALL INDEXES ===")
    
    # Find all index files
    index_files = []
    for table_name in schemaObj.get_table_name_list():
        table_str = table_name.decode('utf-8') if isinstance(table_name, bytes) else str(table_name)
        
        # Check for B-tree index
        btree_file = f"{table_str}.ind"
        if os.path.exists(btree_file):
            index_files.append((table_str, "B-tree", btree_file))
        
        # Check for Hash index
        hash_file = f"{table_str}.hash"
        if os.path.exists(hash_file):
            index_files.append((table_str, "Hash", hash_file))
    
    if not index_files:
        print("No index files found.")
        return
    
    print(f"Found {len(index_files)} index file(s):")
    for table, index_type, filename in index_files:
        file_size = os.path.getsize(filename)
        print(f"  {table} - {index_type} ({filename}) - {file_size} bytes")
    
    confirm = input(f"\nAre you sure you want to delete ALL {len(index_files)} index files? (y/n): ").strip().lower()
    
    if confirm == 'y':
        deleted_count = 0
        for table, index_type, filename in index_files:
            try:
                os.remove(filename)
                print(f"✓ Deleted {table} {index_type} index")
                deleted_count += 1
            except Exception as e:
                print(f"✗ Failed to delete {filename}: {e}")
        
        print(f"\n✅ Successfully deleted {deleted_count}/{len(index_files)} index files")
    else:
        print("Operation cancelled.")

# ------------------------------------------------
# List all existing indexes
# Author: WuShuCheng  2396878284@qq.com
# Input:
#       schemaObj: Schema object containing table metadata
# Output:
#       None (displays list of all index files)
# ------------------------------------------------
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
        print("-" * 80)
        print(f"{'Table':<15} {'Type':<10} {'File':<25} {'Size':<10} {'Status'}")
        print("-" * 80)
        for table_name, index_type, index_file, file_size in found_indexes:
            print(f"{table_name:<15} {index_type:<10} {index_file:<25} {file_size:<10} Active")
        print("-" * 80)
        print(f"Total indexes: {len(found_indexes)}")
        
        # Calculate total storage used
        total_size = sum(size for _, _, _, size in found_indexes)
        print(f"Total storage used: {total_size} bytes ({total_size/1024:.1f} KB)")
    else:
        print("No indexes found.")
        print("💡 Tip: Use option 1 to create and compare index performance!")