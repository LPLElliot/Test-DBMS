#---------------------------------
# head_db.py
# author: Jingyu Han    hjymail@163.com
# modified by: Xinjian Zhang   278254081@qq.com
#--------------------------------------
# the main memory structure of table schema
#------------------------------------
class Header(object): 
    #------------------------
    # constructor of the class
    # input
    #   nameList    : table name list and each element of the list is a triple (table_name,num_of_fields, offset_in_body)
    #   fieldDict   : field dictionary for all tables and each element is (tablename, fieldList) where fieldList is
    #                    a list of fields and each field is a tuple(fieldname,fieldtype,fieldlength
    #   inLen       : number of tables
    #   off         : where the free space begins in body of the schema file
    #---------------------------------------------------------------
    def __init__(self,nameList,fieldDict,inistored, inLen, off):
        'constructor of Header'
        print ('__init__ of Header')
        self.isStored=inistored # whether it is stored
        self.lenOfTableNum=inLen # number of tables
        self.offsetOfBody=off
        self.tableNames=nameList
        self.tableFields=fieldDict
        print ("isStore is ",self.isStored," tableNum is ",self.lenOfTableNum," offset is ",self.offsetOfBody)   
       
    #-----------------------------
    # destructor of the class
    #-------------------------------
    def __del__(self):
        print ('del Header')
        
    #-----------------------------
    # display the schema of all the tables in the schema file
    #----------------------------------------------------------
    def showTables(self):
        if self.lenOfTableNum>0:
            print ("the length of tableNames is",len(self.tableNames))
            for i in range(len(self.tableNames)):
                print(self.tableNames[i])
                print (self.tableFields[i])