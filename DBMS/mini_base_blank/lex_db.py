# ------------------------------------------------
# lex_db.py
# Author: Jingyu Han hjymail@163.com
# Modified by: Xinjian Zhang   278254081@qq.com
# ------------------------------------------------
# This module is responsible for:
# (1) Defining tokens used for parsing SQL statements
# (2) Constructing lexer object for SQL tokenization
# ------------------------------------------------

import ply.lex as lex
import common_db

# Token definitions for SQL keywords and symbols
tokens = (
    'SELECT', 'FROM', 'WHERE', 'AND', 'TCNAME', 'EQX', 'COMMA', 'CONSTANT',
    'STAR', 'SEMI', 'CREATE', 'TABLE', 'INSERT', 'INTO', 'VALUES', 'DELETE', 
    'UPDATE', 'SET', 'DROP', 'CHAR', 'INTEGER', 'LPAREN', 'RPAREN'
)

# ------------------------------------------------
# Token recognition functions
# Each function recognizes a specific SQL keyword or symbol
# ------------------------------------------------

# ------------------------------------------------
# To recognize SELECT keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with SELECT type
# ------------------------------------------------
def t_SELECT(t):
    r"""select"""
    return t

# ------------------------------------------------
# To recognize FROM keyword in SQL statements  
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with FROM type
# ------------------------------------------------
def t_FROM(t):
    r"""from"""
    return t

# ------------------------------------------------
# To recognize WHERE keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with WHERE type
# ------------------------------------------------
def t_WHERE(t):
    r"""where"""
    return t

# ------------------------------------------------
# To recognize AND keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with AND type
# ------------------------------------------------
def t_AND(t):
    r"""and"""
    return t

# ------------------------------------------------
# To recognize CREATE keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with CREATE type
# ------------------------------------------------
def t_CREATE(t):
    r"""create"""
    return t

# ------------------------------------------------
# To recognize TABLE keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with TABLE type
# ------------------------------------------------
def t_TABLE(t):
    r"""table"""
    return t

# ------------------------------------------------
# To recognize INSERT keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with INSERT type
# ------------------------------------------------
def t_INSERT(t):
    r"""insert"""
    return t

# ------------------------------------------------
# To recognize INTO keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with INTO type
# ------------------------------------------------
def t_INTO(t):
    r"""into"""
    return t

# ------------------------------------------------
# To recognize VALUES keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with VALUES type
# ------------------------------------------------
def t_VALUES(t):
    r"""values"""
    return t

# ------------------------------------------------
# To recognize DELETE keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with DELETE type
# ------------------------------------------------
def t_DELETE(t):
    r"""delete"""
    return t

# ------------------------------------------------
# To recognize UPDATE keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with UPDATE type
# ------------------------------------------------
def t_UPDATE(t):
    r"""update"""
    return t

# ------------------------------------------------
# To recognize SET keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with SET type
# ------------------------------------------------
def t_SET(t):
    r"""set"""
    return t

# ------------------------------------------------
# To recognize DROP keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with DROP type
# ------------------------------------------------
def t_DROP(t):
    r"""drop"""
    return t

# ------------------------------------------------
# To recognize CHAR data type keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with CHAR type
# ------------------------------------------------
def t_CHAR(t):
    r"""char"""
    return t

# ------------------------------------------------
# To recognize INTEGER data type keyword in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with INTEGER type
# ------------------------------------------------
def t_INTEGER(t):
    r"""integer"""
    return t

# ------------------------------------------------
# To recognize left parenthesis in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with LPAREN type
# ------------------------------------------------
def t_LPAREN(t):
    r"""\("""
    return t

# ------------------------------------------------
# To recognize right parenthesis in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with RPAREN type
# ------------------------------------------------
def t_RPAREN(t):
    r"""\)"""
    return t

# ------------------------------------------------
# To recognize comma separator in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with COMMA type
# ------------------------------------------------
def t_COMMA(t):
    r""","""
    return t

# ------------------------------------------------
# To recognize equality operator in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with EQX type
# ------------------------------------------------
def t_EQX(t):
    r"""="""
    return t

# ------------------------------------------------
# To recognize asterisk symbol in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with STAR type
# ------------------------------------------------
def t_STAR(t):
    r"""\*"""
    return t

# ------------------------------------------------
# To recognize semicolon in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with SEMI type
# ------------------------------------------------
def t_SEMI(t):
    r""";"""
    return t

# ------------------------------------------------
# To recognize constants (strings and numbers) in SQL statements
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with CONSTANT type
# ------------------------------------------------
def t_CONSTANT(t):
    r"""'[^']*'|[0-9]+"""
    return t

# ------------------------------------------------
# To recognize table/column names and handle reserved keywords
# Input:
#       t: token object from PLY lexer
# Output:
#       token object with appropriate type (keyword or TCNAME)
# ------------------------------------------------
def t_TCNAME(t):
    r"""[a-zA-Z_][a-zA-Z0-9_]*"""
    # Reserved keyword dictionary
    reserved = {
        'select': 'SELECT', 'from': 'FROM', 'where': 'WHERE', 'and': 'AND',
        'create': 'CREATE', 'table': 'TABLE', 'insert': 'INSERT', 'into': 'INTO',
        'values': 'VALUES', 'delete': 'DELETE', 'update': 'UPDATE', 'set': 'SET',
        'drop': 'DROP', 'char': 'CHAR', 'integer': 'INTEGER'
    }
    t.type = reserved.get(t.value.lower(), 'TCNAME')
    return t

# Ignored characters (whitespace and tabs)
t_ignore = ' \t'

# ------------------------------------------------
# To handle newline characters and track line numbers
# Input:
#       t: token object containing newline
# Output:
#       None (updates line counter)
# ------------------------------------------------
def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)

# ------------------------------------------------
# Error handling for illegal characters
# Input:
#       t: token object containing illegal character
# Output:
#       None (prints error and skips character)
# ------------------------------------------------
def t_error(t):
    print(f"Illegal character '{t.value[0]}'")
    t.lexer.skip(1)

# ------------------------------------------------
# Initialize lexer and set global lexer variable
# Input:
#       None
# Output:
#       None (sets common_db.global_lexer)
# ------------------------------------------------
def set_lex_handle():
    """Initialize the lexer and set it to global_lexer"""
    common_db.global_lexer = lex.lex()
    if common_db.global_lexer is None:
        print('Error: LEX lexer object could not be created')