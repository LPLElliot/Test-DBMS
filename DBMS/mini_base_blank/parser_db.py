# ------------------------------------------------
# parser_db.py
# Author: Jingyu Han   hjymail@163.com
# Modified by: Xinjian Zhang   278254081@qq.com   
# ------------------------------------------------
# This module constructs syntax trees for SQL statements
# The output is a syntax tree representing the parsed SQL
# ------------------------------------------------
import common_db
import ply.yacc as yacc
from lex_db import tokens

# Start symbol for grammar
start = 'Statement'

# ------------------------------------------------
# To parse top-level SQL statements
# Input:
#       p: parser object containing statement tokens
# Output:
#       syntax tree node for the statement
# ------------------------------------------------
def p_statement(p):
    '''Statement : SfwQuery
                 | CreateQuery
                 | InsertQuery
                 | DeleteQuery
                 | UpdateQuery
                 | DropQuery'''
    p[0] = p[1]
    common_db.global_syn_tree = p[0]

# ------------------------------------------------
# To parse SELECT-FROM-WHERE queries
# Input:
#       p: parser object containing SFW tokens
# Output:
#       syntax tree node for SFW query
# ------------------------------------------------
def p_sfw_query(p):
    'SfwQuery : SFW'
    p[0] = p[1]

# ------------------------------------------------
# To parse complete SFW structure
# Input:
#       p: parser object containing SELECT, FROM, WHERE components
# Output:
#       syntax tree node for SFW structure
# ------------------------------------------------
def p_sfw(p):
    '''SFW : SELECT SelList FROM FromList opt_where opt_semi'''
    p[1] = common_db.Node('SELECT', None)
    p[3] = common_db.Node('FROM', None)
    p[0] = common_db.Node('SFW', [p[1], p[2], p[3], p[4], p[5]])

# ------------------------------------------------
# To parse optional WHERE clause
# Input:
#       p: parser object containing optional WHERE tokens
# Output:
#       syntax tree node for WHERE clause or None
# ------------------------------------------------
def p_opt_where(p):
    '''opt_where : WHERE Condition
                 | empty'''
    if len(p) == 3:
        p[0] = common_db.Node('WHERE', [p[2]])
    else:
        p[0] = None

# ------------------------------------------------
# To parse condition expressions (single or multiple with AND)
# Input:
#       p: parser object containing condition tokens
# Output:
#       syntax tree node for condition
# ------------------------------------------------
def p_condition(p):
    '''Condition : SimpleCond
                 | SimpleCond AND Condition'''
    if len(p) == 2:
        p[0] = p[1]
    else:
        p[0] = common_db.Node('AND_OP', [p[1], p[3]])

# ------------------------------------------------
# To parse simple condition (column = value/column)
# Input:
#       p: parser object containing simple condition tokens
# Output:
#       syntax tree node for simple condition
# ------------------------------------------------
def p_simple_cond(p):
    '''SimpleCond : TCNAME EQX CONSTANT
                  | TCNAME EQX TCNAME'''
    p[1] = common_db.Node('TCNAME', [p[1]])
    p[2] = common_db.Node('=', None)
    if p.slice[3].type == 'CONSTANT':
        p[3] = common_db.Node('CONSTANT', [p[3]])
    else:
        p[3] = common_db.Node('TCNAME', [p[3]])
    p[0] = common_db.Node('Cond', [p[1], p[2], p[3]])

# ------------------------------------------------
# To parse optional semicolon
# Input:
#       p: parser object containing optional semicolon
# Output:
#       None (semicolon is optional)
# ------------------------------------------------
def p_opt_semi(p):
    '''opt_semi : SEMI
                | empty'''
    pass

# ------------------------------------------------
# To parse SELECT list (recursive, multiple columns)
# Input:
#       p: parser object containing column list tokens
# Output:
#       syntax tree node for SELECT list
# ------------------------------------------------
def p_sel_list_recursive(p):
    'SelList : TCNAME COMMA SelList'
    p[1] = common_db.Node('TCNAME', [p[1]])
    p[2] = common_db.Node(',', None)
    p[0] = common_db.Node('SelList', [p[1], p[2], p[3]])

# ------------------------------------------------
# To parse SELECT list (single column)
# Input:
#       p: parser object containing single column token
# Output:
#       syntax tree node for single column SELECT
# ------------------------------------------------
def p_sel_list_single(p):
    'SelList : TCNAME'
    p[1] = common_db.Node('TCNAME', [p[1]])
    p[0] = common_db.Node('SelList', [p[1]])

# ------------------------------------------------
# To parse SELECT * (all columns)
# Input:
#       p: parser object containing star token
# Output:
#       syntax tree node for SELECT *
# ------------------------------------------------
def p_sel_list_star(p):
    'SelList : STAR'
    p[1] = common_db.Node('STAR', None)
    p[0] = common_db.Node('SelList', [p[1]])

# ------------------------------------------------
# To parse FROM list (recursive, multiple tables)
# Input:
#       p: parser object containing table list tokens
# Output:
#       syntax tree node for FROM list
# ------------------------------------------------
def p_from_list_recursive(p):
    'FromList : TCNAME COMMA FromList'
    p[1] = common_db.Node('TCNAME', [p[1]])
    p[2] = common_db.Node(',', None)
    p[0] = common_db.Node('FromList', [p[1], p[2], p[3]])

# ------------------------------------------------
# To parse FROM list (single table)
# Input:
#       p: parser object containing single table token
# Output:
#       syntax tree node for single table FROM
# ------------------------------------------------
def p_from_list_single(p):
    'FromList : TCNAME'
    p[1] = common_db.Node('TCNAME', [p[1]])
    p[0] = common_db.Node('FromList', [p[1]])

# ------------------------------------------------
# To parse CREATE TABLE statements
# Input:
#       p: parser object containing CREATE TABLE tokens
# Output:
#       syntax tree node for CREATE TABLE
# ------------------------------------------------
def p_create_query(p):
    'CreateQuery : CREATE TABLE TCNAME LPAREN FieldDefs RPAREN opt_semi'
    p[0] = common_db.Node('CREATE_TABLE', None, varList={'table_name': p[3], 'fields': p[5]})

# ------------------------------------------------
# To parse field definitions in CREATE TABLE statements
# Input:
#       p: parser object containing field definitions
# Output:
#       list of field definitions
# ------------------------------------------------
def p_field_defs(p):
    '''FieldDefs : FieldDef COMMA FieldDefs
                 | FieldDef'''
    if len(p) == 4:
        p[0] = [p[1]] + p[3]
    else:
        p[0] = [p[1]]

# ------------------------------------------------
# To parse individual field definition
# Input:
#       p: parser object containing field name and data type
# Output:
#       dictionary with field information
# ------------------------------------------------
def p_field_def(p):
    'FieldDef : TCNAME DataType'
    field_name = p[1]
    data_type_info = p[2]
    field_type_str, length = data_type_info
    type_code = 0
    resolved_length = 0
    if field_type_str.upper() == 'CHAR':
        type_code = 0
        resolved_length = int(length) if length is not None else 255
    elif field_type_str.upper() == 'INTEGER':
        type_code = 2
        resolved_length = length if length is not None else 10
    p[0] = {'name': field_name, 'type_code': type_code, 'length': resolved_length}

# ------------------------------------------------
# To parse data types (CHAR and INTEGER)
# Input:
#       p: parser object containing data type tokens
# Output:
#       tuple with type name and length
# ------------------------------------------------
def p_data_type(p):
    '''DataType : CHAR LPAREN CONSTANT RPAREN
                | INTEGER'''
    if p[1].upper() == 'CHAR':
        length_str = p[3].strip("'") if p[3].startswith("'") and p[3].endswith("'") else p[3]
        p[0] = (p[1], int(length_str))
    elif p[1].upper() == 'INTEGER':
        p[0] = (p[1], 10)

# ------------------------------------------------
# To parse INSERT INTO statements
# Input:
#       p: parser object containing INSERT tokens
# Output:
#       syntax tree node for INSERT INTO
# ------------------------------------------------
def p_insert_query(p):
    'InsertQuery : INSERT INTO TCNAME VALUES LPAREN ValueList RPAREN opt_semi'
    p[0] = common_db.Node('INSERT_INTO', None, varList={'table_name': p[3], 'values': p[6]})

# ------------------------------------------------
# To parse value lists in INSERT statements
# Input:
#       p: parser object containing values
# Output:
#       list of values
# ------------------------------------------------
def p_value_list(p):
    '''ValueList : CONSTANT COMMA ValueList
                 | CONSTANT'''
    if len(p) == 4:
        val = p[1].strip("'") if p[1].startswith("'") and p[1].endswith("'") else p[1]
        p[0] = [val] + p[3]
    else:
        val = p[1].strip("'") if p[1].startswith("'") and p[1].endswith("'") else p[1]
        p[0] = [val]

# ------------------------------------------------
# To parse DELETE FROM statements
# Input:
#       p: parser object containing DELETE tokens
# Output:
#       syntax tree node for DELETE FROM
# ------------------------------------------------
def p_delete_query(p):
    '''DeleteQuery : DELETE FROM TCNAME opt_where opt_semi'''
    condition_node = None
    if p[4] and p[4].value == 'WHERE' and p[4].children:
        condition_node = p[4].children[0]
    p[0] = common_db.Node('DELETE_FROM', None, varList={'table_name': p[3], 'condition': condition_node})

# ------------------------------------------------
# To parse UPDATE SET statements
# Input:
#       p: parser object containing UPDATE tokens
# Output:
#       syntax tree node for UPDATE SET
# ------------------------------------------------
def p_update_query(p):
    'UpdateQuery : UPDATE TCNAME SET AssignmentList opt_where opt_semi'
    condition_node = None
    if p[5] and p[5].value == 'WHERE' and p[5].children:
        condition_node = p[5].children[0]
    p[0] = common_db.Node('UPDATE_SET', None, varList={'table_name': p[2], 'assignments': p[4], 'condition': condition_node})

# ------------------------------------------------
# To parse assignment lists in UPDATE statements
# Input:
#       p: parser object containing assignments
# Output:
#       list of assignment dictionaries
# ------------------------------------------------
def p_assignment_list(p):
    '''AssignmentList : Assignment COMMA AssignmentList
                      | Assignment'''
    if len(p) == 4:
        p[0] = [p[1]] + p[3]
    else:
        p[0] = [p[1]]

# ------------------------------------------------
# To parse individual assignments (column = value)
# Input:
#       p: parser object containing assignment tokens
# Output:
#       dictionary with column and value
# ------------------------------------------------
def p_assignment(p):
    'Assignment : TCNAME EQX CONSTANT'
    val = p[3].strip("'") if p[3].startswith("'") and p[3].endswith("'") else p[3]
    p[0] = {'column': p[1], 'value': val}

# ------------------------------------------------
# To parse DROP TABLE statements
# Input:
#       p: parser object containing DROP tokens
# Output:
#       syntax tree node for DROP TABLE
# ------------------------------------------------
def p_drop_query(p):
    'DropQuery : DROP TABLE TCNAME opt_semi'
    p[0] = common_db.Node('DROP_TABLE', None, varList={'table_name': p[3]})

# ------------------------------------------------
# To handle empty productions
# Input:
#       p: parser object (empty)
# Output:
#       None
# ------------------------------------------------
def p_empty(p):
    'empty :'
    pass

# ------------------------------------------------
# Error handling for syntax errors
# Input:
#       p: parser object containing error token
# Output:
#       None (prints error message)
# ------------------------------------------------
def p_error(p):
    if p:
        print(f'Syntax error at token {p.type}')
    else:
        print('Syntax error at EOF')

# ------------------------------------------------
# Initialize parser and set global parser variable
# Input:
#       None
# Output:
#       None (sets common_db.global_parser)
# ------------------------------------------------
def set_parser_handle():
    common_db.global_parser = yacc.yacc(write_tables=0)
    if common_db.global_parser is None:
        print('Error: YACC parser object could not be created')