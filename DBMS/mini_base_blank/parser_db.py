#-----------------------------
# parser_db.py
# author: Jingyu Han   hjymail@163.com
# modified by: Xinjian Zhang   278254081@qq.com
#-------------------------------
# the module is to construct a syntax tree for a "select from where" SQL clause
# the output is a syntax tree
#----------------------------------------------------
import common_db
# the following two packages need to be installed by yourself
import ply.yacc as yacc 
import ply.lex as lex
from lex_db import tokens
#---------------------------------
# Query  : SFW
#   SWF  : SELECT SelList FROM FromList WHERE Condition
# SelList: TCNAME COMMA SelList
# SelList: TCNAME
#
# FromList:TCNAME COMMA FromList
# FromList:TCNAME
# Condition: TCNAME EQX CONSTANT
#---------------------------------
#------------------------------
# check the syntax tree
# input:
#       syntax tree
# output:
#       true or falise
#-----------------------------
def check_syn_tree(syn_tree):
    if syn_tree:
        pass
#------------------------------
#(1) construct the node for query expression
#(2) check the tree
#(3) view the data in the tree
# input:
#       
# output:
#       the root node of syntax tree
#--------------------------------------      
def p_expr_query(t):
    'Query : SFW'
    t[0]=common_db.Node('Query',[t[1]])
    common_db.global_syn_tree=t[0]
    check_syn_tree(common_db.global_syn_tree)
    common_db.show(common_db.global_syn_tree)
    return t
#------------------------------
#(1) construct the node for WFW expression
# input:
#       
# output:
#       the nodes
#--------------------------------------   
def p_expr_swf(t):
    'SFW : SELECT SelList FROM FromList WHERE Cond'
    t[1]=common_db.Node('SELECT',None)
    t[3]=common_db.Node('FROM',None)
    t[5]=common_db.Node('WHERE',None)
    t[0]=common_db.Node('SFW',[t[1],t[2],t[3],t[4],t[5],t[6]])
    return t
#------------------------------
#construct the node for select list
# input:
#       
# output:
#       the nodes
#--------------------------------------   
def p_expr_sellist_first(t):
    'SelList : TCNAME COMMA SelList'
    t[1]=common_db.Node('TCNAME',[t[1]])
    t[2]=common_db.Node(',',None)
    t[0]=common_db.Node('SelList',[t[1],t[2],t[3]])
    return t
#------------------------------
#construct the node for select list expression
# input:
#       
# output:
#       the nodes
#--------------------------------------   
def p_expr_sellist_second(t):
    'SelList : TCNAME'
    t[1]=common_db.Node('TCNAME',[t[1]])
    t[0]=common_db.Node('SelList',[t[1]])
    return t
#---------------------------
#construct the node for from expression
# input:
#       
# output:
#       the nodes
#--------------------------------------   
def p_expr_fromlist_first(t):
    'FromList : TCNAME COMMA FromList'
    t[1]=common_db.Node('TCNAME',[t[1]])
    t[2]=common_db.Node(',',None)
    t[0]=common_db.Node('FromList',[t[1],t[2],t[3]])
    return t
#------------------------------
#(1) construct the node for from expression
# input:
#       
# output:
#       the nodes
#--------------------------------------           
def p_expr_fromlist_second(t):
    'FromList : TCNAME'
    t[1]=common_db.Node('TCNAME',[t[1]])
    t[0]=common_db.Node('FromList',[t[1]])    
    return t
#------------------------------
#construct the node for condition expression
# input:
#       
# output:
#       the nodes
#--------------------------------------   
def p_expr_condition(t):
    'Cond : TCNAME EQX CONSTANT'
    t[1]=common_db.Node('TCNAME',[t[1]])
    t[2]=common_db.Node('=',None)
    t[3]=common_db.Node('CONSTANT',[t[3]])
    t[0]=common_db.Node('Cond',[t[1],t[2],t[3]])
    return t 
#------------------------------
# for error
# input:
#       
# output:
#       the error messages
#--------------------------------------   
def p_error(t):
    print ('wrong at %s'% t.value)
#------------------------------------------
# to set the global_parser handle in common_db.py
#---------------------------------------------    
def set_handle():    
    common_db.global_parser=yacc.yacc(write_tables=0)
    if common_db.global_parser is None:
        print ('wrong when yacc object is created')