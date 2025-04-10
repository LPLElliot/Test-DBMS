#----------------------------------------
# common_db.py
# author: Jingyu Han   hjymail@163.com
# modified by:
#--------------------------------------------
# the module provides the constants, class, data structures which
# are used for all the program
#--------------------------------------------------
BLOCK_SIZE=4096 # the size of one block during reading files
global_lexer=None   # the global lex, which is filled in the moudle lex_db.py
global_parser=None  # the global yacc, which is filled in the module yacc_db.py
global_syn_tree=None # the global syntax tree, which is filled in parser_db.py
global_logical_tree=None # global variable, which is to store the logical query plan tree

#-----------------------------
# the following is the structure of tree node
#---------------------------------
class Node:
    def __init__(self,value,children,varList=None):
        self.value=value
        self.var=varList
        if children:
            self.children=children
        else:
            self.children=[]
            
#-------------------------
# show() function is to traverse through the tree
#---------------------------
def show(node_obj):
    if isinstance(node_obj,Node):# it is a Node object
        print (node_obj.value)
        if node_obj.var:
            print (node_obj.var)
        if node_obj.children:
            for i in range(len(node_obj.children)):
                show(node_obj.children[i])
    if isinstance(node_obj,str):# it is a string object
        print (node_obj)