#-------------------------------
# lex_db.py
# author: Jingyu Han hjymail@163.com
# modified by: Xinjian Zhang   278254081@qq.com
#--------------------------------------------
# the module is responsible for
#(1) defining tokens used for parsing SQL statements
#(2) constructing a lex object
#-------------------------------
import ply.lex as lex
import common_db
import re

tokens = (
    'SELECT', 'FROM', 'WHERE', 'AND', 'TCNAME', 'EQX', 'COMMA', 'CONSTANT',
    'STAR', 'SEMI', 'CREATE', 'TABLE', 'INSERT', 'INTO', 'VALUES', 'DELETE', 
    'UPDATE', 'SET', 'DROP', 'CHAR', 'INTEGER', 'LPAREN', 'RPAREN'
)

# Rules for keywords and symbols
def t_SELECT(t):
    r"""select"""
    return t

def t_FROM(t):
    r"""from"""
    return t

def t_WHERE(t):
    r"""where"""
    return t

def t_AND(t):
    r"""and"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize CREATE keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with CREATE type
# ------------------------------------------------
def t_CREATE(t):
    r"""create"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize TABLE keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with TABLE type
# ------------------------------------------------
def t_TABLE(t):
    r"""table"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize INSERT keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with INSERT type
# ------------------------------------------------
def t_INSERT(t):
    r"""insert"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize INTO keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with INTO type
# ------------------------------------------------
def t_INTO(t):
    r"""into"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize VALUES keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with VALUES type
# ------------------------------------------------
def t_VALUES(t):
    r"""values"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize DELETE keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with DELETE type
# ------------------------------------------------
def t_DELETE(t):
    r"""delete"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize UPDATE keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with UPDATE type
# ------------------------------------------------
def t_UPDATE(t):
    r"""update"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize SET keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with SET type
# ------------------------------------------------
def t_SET(t):
    r"""set"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize DROP keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with DROP type
# ------------------------------------------------
def t_DROP(t):
    r"""drop"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize CHAR data type keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with CHAR type
# ------------------------------------------------
def t_CHAR(t):
    r"""char"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize INTEGER data type keyword in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with INTEGER type
# ------------------------------------------------
def t_INTEGER(t):
    r"""integer"""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize left parenthesis in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with LPAREN type
# ------------------------------------------------
def t_LPAREN(t):
    r"""\("""
    return t

# ----------------------------------------------
# Author: Xinjian Zhang
# to recognize right parenthesis in SQL statements
# input
#       t: token object from PLY lexer
# output
#       token object with RPAREN type
# ------------------------------------------------
def t_RPAREN(t):
    r"""\)"""
    return t

def t_COMMA(t):
    r""","""
    return t

def t_EQX(t):
    r"""="""
    return t

def t_STAR(t):
    r"""\*"""
    return t

def t_SEMI(t):
    r""";"""
    return t

# Rule for constants
def t_CONSTANT(t):
    r"""'[^']*'|'[^']*'|\d+"""
    return t

# Rule for identifiers (Table/Column Names)
def t_TCNAME(t):
    r"""[a-zA-Z_][a-zA-Z0-9_]*"""
    reserved = {
        'select': 'SELECT', 'from': 'FROM', 'where': 'WHERE', 'and': 'AND',
        'create': 'CREATE', 'table': 'TABLE', 'insert': 'INSERT', 'into': 'INTO',
        'values': 'VALUES', 'delete': 'DELETE', 'update': 'UPDATE', 'set': 'SET',
        'drop': 'DROP', 'char': 'CHAR', 'integer': 'INTEGER'
    }
    t.type = reserved.get(t.value.lower(), 'TCNAME')
    return t

# Ignored characters
t_ignore = ' \t'

def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)

def t_error(t):
    print(f"Illegal character '{t.value[0]}'")
    t.lexer.skip(1)

def set_lex_handle():
    common_db.global_lexer = lex.lex(reflags=re.IGNORECASE)
    if common_db.global_lexer is None:
        print('Error: Lexer could not be created in lex_db.py.')