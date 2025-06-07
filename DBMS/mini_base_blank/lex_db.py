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

def t_CREATE(t):
    r"""create"""
    return t

def t_TABLE(t):
    r"""table"""
    return t

def t_INSERT(t):
    r"""insert"""
    return t

def t_INTO(t):
    r"""into"""
    return t

def t_VALUES(t):
    r"""values"""
    return t

def t_DELETE(t):
    r"""delete"""
    return t

def t_UPDATE(t):
    r"""update"""
    return t

def t_SET(t):
    r"""set"""
    return t

def t_DROP(t):
    r"""drop"""
    return t

def t_CHAR(t):
    r"""char"""
    return t

def t_INTEGER(t):
    r"""integer"""
    return t

def t_LPAREN(t):
    r"""\("""
    return t

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

# Rule for constants - 修复单引号字符串识别
def t_CONSTANT(t):
    r"""'[^']*'|\d+"""
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