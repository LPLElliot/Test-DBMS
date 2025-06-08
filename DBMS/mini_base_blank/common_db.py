# ------------------------------------------------
# common_db.py
# Author: Jingyu Han   hjymail@163.com
# Modified by: Xinjian Zhang   278254081@qq.com
# ------------------------------------------------
# This module provides constants, classes, and data structures
# that are used throughout the entire program
# ------------------------------------------------

# Constants
BLOCK_SIZE = 4096  # The size of one block during file reading operations

# Global variables
global_lexer = None         # Global lexer object, initialized in lex_db.py
global_parser = None        # Global parser object, initialized in parser_db.py
global_syn_tree = None      # Global syntax tree, created in parser_db.py
global_logical_tree = None  # Global logical query plan tree

# ------------------------------------------------
# Node class for tree data structure
# This class represents nodes in syntax trees and logical query plans
# ------------------------------------------------
class Node:
    # ------------------------------------------------
    # Constructor for Node class
    # Input:
    #       value: the value/type of the node
    #       children: list of child nodes
    #       varList: optional dictionary of variables
    # Output:
    #       Node object
    # ------------------------------------------------
    def __init__(self, value, children, varList=None):
        self.value = value      # Node type or value
        self.var = varList      # Variable dictionary for storing node data
        if children:
            self.children = children
        else:
            self.children = []  # Initialize empty children list

# ------------------------------------------------
# Tree traversal function
# Input:
#       node_obj: Node object or string to display
# Output:
#       None (prints tree structure)
# ------------------------------------------------
def show(node_obj):
    if isinstance(node_obj, Node):  # Check if it's a Node object
        print(node_obj.value)       # Print node value
        if node_obj.var:            # Print variables if they exist
            print(node_obj.var)
        if node_obj.children:       # Recursively traverse children
            for i in range(len(node_obj.children)):
                show(node_obj.children[i])
    if isinstance(node_obj, str):   # Check if it's a string object
        print(node_obj)             # Print string directly