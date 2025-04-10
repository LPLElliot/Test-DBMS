#-------------------------------
# lex_db.py
# author: Jingyu Han hjymail@163.com
# modified by:
#--------------------------------------------
# the module is responsible for
#(1) defining tokens used for parsing SQL statements
#(2) constructing a lex object
#-------------------------------
import ply.lex as lex
import common_db
tokens=('SELECT','FROM','WHERE','AND','TCNAME','EQX','COMMA','CONSTANT','SPACE')
# the following is to defining rules for each token
def t_SELECT(t):
    r'select'
    return t
def t_FROM(t):
    r'from'
    return t
def t_WHERE(t):
    r'where'
    return t
def t_AND(t):
    r'and'
    return t
def t_TCNAME(t):
    r'[A-Z_a-z]\w*'
    return t
def t_COMMA(t):
    r','
    return t
def t_EQX(t):
    r'[=]'
    return t
def t_CONSTANT(t):
    r'\d+|\'\w+\''
    return t
def t_SPACE(t):
    r'\s+'
    pass

#--------------------------
# to cope with the error
#------------------------
def t_error(t):
    try:
    
        print ('wrong')
    except lex.LexError:
        print ('wrong')
    else:
        print ('wrong')

#------------------------------------------
# to set the global_lexer in common_db.py
#-------------------------------------------
def set_lex_handle():
    common_db.global_lexer=lex.lex()
    if common_db.global_lexer is None:
        print ('wrong when the global_lex is created')

'''
def test():
    my_lexer=lex.lex()
    my_lexer.input("select f1,f2 from GOOD where f1='xx' and f2=5 ")
    while True:
        temp_tok=my_lexer.token()
        if temp_tok is None:
            break
        print temp_tok

test()
'''