'''
index_db.py
in this module, B tree is implemented
'''
import struct
# The 0 block stores the meta information of the tree
'''
block_id|has_root|num_of_levels|root_node_ptr
# note: the root_node_ptr is a block id
'''
MAX_NUM_OF_KEYS=200#the number of keys in each block
# structure of leaf node
'''
block_id|node_type|number_of_keys|key_0|ptr_0|...|key_i|ptr_i|...|key_n|ptr_n|...free space...|last_ptr
note: for leaf node, ptr is a block id+entry id (8 bytes) except for the last one
'''
LEAF_NODE_TYPE=1
LEN_OF_LEAF_NODE=10+4+4  # key takes 10 bytes, block_id takes 4 bytes and offset takes 4 bytes
# structure of internal node
'''
block_id|node_type|number_of_keys|key_0|ptr_0|key_1|ptr_1|...|key_n|ptr_n|...free space...|last_ptr|
note: For internal node, ptr is just a block id( 4 bytes) 
'''
INTERNAL_NODE_TYPE=0
SPECIAL_INDEX_BLOCK_PTR=-1 # this is the last ptr for last leaf node when the next node is unknown

import os
import common_db
import ctypes

def test():
    my_dict={}
    my_dict.setdefault('one',80)
    my_dict.setdefault('two',90)
    my_dict.setdefault('aaa',90)
    print (my_dict.keys())
    print (my_dict.items())
    for my_each_key in sorted(my_dict):
        print ("the value of key ",my_each_key," is ",my_dict[my_each_key])
    my_list=[]
    my_tuple=(1,2)
    my_list.append(my_tuple)
    (a,b)=my_list[0]
    print (a,b)
    
class Index(object):
    #------------------------------------
    # constructor of the class
    # input
    #       tablename : the table to be indexed
    #-----------------------------------------
    def __init__(self,tablename):
        print ("__init__ of ",Index.__name__)
        tablename.strip()
        if  not os.path.exists(tablename+'.ind'): # in this case, the index file does not exist
            print ('index file '+tablename+'.ind does not exist')
            self.f_handle=open(tablename+'.ind','wb+')
            print (tablename+'.ind has been created')
        else: # the index file exists and we read its first block
            self.f_handle=open(tablename+'.ind','rb+')
            print ('index file '+tablename+'.ind has been opened')
            self.open=True
            self.first_block_buf=ctypes.create_string_buffer(common_db.BLOCK_SIZE)
            self.f_handle.seek(0)
            self.first_block_buf=self.f_handle.read(common_db.BLOCK_SIZE)
            # to view all the index entries
            # to be inserted here
            
    #---------------------------------
    # destructor of the class
    #-----------------------------------
    def __del__(self):
        print ("__del__ of ",Index.__name__)
        self.f_handle.close()
        self.open=False
    
    #-----------------------------
    # create index for all indexed items in one run
    #-----------------------------------
    def create_index(self,index_field):
        print ('create_index begins to execute')
        #field_value_address=[] # its element is a tuple (field_value,address)
        # to be inserted here

    #-----------------------------
    # get the internal node to follow
    # input
    #       current_value:
    #       index_key_list:
    #       index_ptr_list:
    #output
    #       the block_id to follow
    #--------------------------------
    def get_next_block_ptr(self,current_value,index_key_list,index_ptr_list):
        ret_value=-1
        return ret_value
    
    #---------------------------------
    # insert the index entry into main memory list, which needs to determine the poistion
    # input
    #       inert_key
    #       insert_block_id
    #       insert_oofset
    # output 
    #       key_list
    #       ptr_list    : of which each element is a tuple (block_id,offset_id)
    def insert_key_value_into_leaf_list(self,insert_key,ptr_tuple,key_list,ptr_list):
        if len(key_list)>0:
            pos=-1
            for i in range(len(key_list)):
                current_key=key_list[i]
                if current_key==insert_key:
                    pos=i
                    break
                elif current_key>insert_key:
                    pos=i
                    break
            if pos==-1:
                pos=len(key_list)-1
            key_list.insert(pos,insert_key)           
            ptr_list.insert(pos,ptr_tuple)                 
        elif len(key_list)==0:
            key_list.append(insert_key)
            ptr_list.append(ptr_tuple)  

    #-------------------------------
    # to insert a index entry into the index file
    # input
    #       field_value     # field value
    #       block_id        # block id
    #       offset          # offset in offset table, it is an integer
    #--------------------------------------
    def insert_index_entry(self,field_value,block_id,offset):
        print ('insert_index_entry begins to execute')
        if len(field_value.strip())>0 and block_id>0 and offset>0:# the following is to insert an index entry into the index file
            if len(self.first_block_buf.strip())==0:# there is no data in the index file
                # to prepare the data in the index node, which is stored in block 1 
                first_index_block=ctypes.create_string_buffer(common_db.BLOCK_SIZE)
                #block_id|node_type|number_of_keys|key_0|ptr_0
                struct.pack_into('!iii10sii',first_index_block,0,1,LEAF_NODE_TYPE,1,field_value,block_id,offset)
                struct.pack_into('!i',first_index_block,common_db.BLOCK_SIZE-struct.calcsize('!i'),SPECIAL_INDEX_BLOCK_PTR)       
                # to prepare the meta block node, which is stored in block 0
                self.meta_index_block=ctypes.create_string_buffer(common_db.BLOCK_SIZE)
                struct.pack_into('!i?ii',self.meta_index_block,0,0,True,1,1) #block_id,has_root,number of levels,root_node_ptr(block_id)
                # record the meta information in the main memory data structures
                self.has_root=True
                self.number_of_levels=1
                self.root_node_ptr=1
                # the following is to write data to index file
                self.f_handle.seek(0)
                self.f_handle.write(self.meta_index_block)
                self.f_handle.write(first_index_block)
                self.f_handle.flush()
            else:# there is data in the file
                self.meta_index_block=ctypes.create_string_buffer(common_db.BLOCK_SIZE)
                self.f_handle.seek(0)
                self.meta_index_block=self.f_handle.read(common_db.BLOCK_SIZE)
                temp_block_id,self.has_root,self.num_of_levels,self.root_node_ptr=struct.unpack_from('!i?ii',self.meta_index_block,0)
                if self.has_root==True and self.num_of_levels>0 and self.root_node_ptr>0:
                    temp_count=0
                    next_node_ptr=self.root_node_ptr
                    while(temp_count<self.num_of_levels-1):# to search through the internal nodes
                        current_index_block=ctypes.create_string_buffer(common_db.BLOCK_SIZE)
                        read_pos=next_node_ptr*common_db.BLOCK_SIZE # the begining of the target block
                        self.f_handle.seek(read_pos)
                        current_index_block=self.f_handle.read(common_db.BLOCK_SIZE)
                        current_node_type,current_num_of_keys=struct.unpack_from('!ii',current_index_block,struct.calcsize('!i'))
                        if current_node_type!=INTERNAL_NODE_TYPE:
                            print ('the internal node type is wrong')
                            return
                        if current_num_of_keys <=0:
                            print ('the current_num_of_keys is wrong in internal node')
                            return
                        internal_key_list=[]
                        internal_ptr_list=[]
                        key_list=[]
                        ptr_list=[]
                        for i in range(current_num_of_keys):
                            current_key,current_ptr=struct.unpack_from('!10si',current_index_block,struct.calcsize('!iii')+i*(10+4))
                            internal_key_list.append(current_key)
                            internal_ptr_list.append(current_ptr)
                        last_ptr,=struct.unpack_from('!i',current_index_block,common_db.BLOCK_SIZE-4)
                        ptr_list.append(last_ptr)
                        # now it is to determine which path we should follow
                        next_node_ptr= self.get_next_block_ptr(field_value, key_list, ptr_list)
                        temp_count+=1
                    # now it is at the leaf node
                    current_index_block=ctypes.create_string_buffer(common_db.BLOCK_SIZE)
                    read_pos=next_node_ptr*common_db.BLOCK_SIZE    # where the leaf node lies
                    self.f_handle.seek(read_pos)
                    current_index_block=self.f_handle.read(common_db.BLOCK_SIZE)
                    current_node_type,current_num_of_keys=struct.unpack_from('!ii',current_index_block,struct.calcsize('!i'))
                    if current_node_type==LEAF_NODE_TYPE:# it is leaf node
                        if current_num_of_keys<MAX_NUM_OF_KEYS:# insert the value into the leaf node
                            # the following is to read index entry into main memory list
                            key_list=[]
                            ptr_list=[]
                            for i in range(current_num_of_keys):
                                current_key,block_ptr,current_offset=struct.unpack_from('!10sii',current_index_block,struct.calcsize('!iii')+i*LEN_OF_LEAF_NODE)
                                key_list.append(current_key)
                                my_tuple=(block_ptr,current_offset)
                                ptr_list.append(my_tuple)
                            self.insert_key_value_into_leaf_list(field_value, my_tuple, key_list, ptr_list)
                            # the following is to write the new index entry list to buffer
                            for i in range(len(key_list)):
                                curent_key=key_list[i]
                                (current_id,current_offset)=ptr_list[i]
                                struct.pack_into('!10sii',current_index_block,struct.calcsize('!iii')+i*LEN_OF_LEAF_NODE,current_key,current_id,current_offset)
                            # change the nmber_of_keys
                            current_num_of_keys+=1
                            struct.pack_into('!i',current_index_block,8,current_num_of_keys)
                            self.f_handle.seek(read_pos)
                            self.f_handle.write(current_index_block)
                            self.f_handle.flush()                      
                        else:
                            print ("the leaf node is full, we should split")
                    else:
                        print ('wrong, it is should be a leaf node')
                else:
                    print ('the information in the index file is wrong')
            
            
            
        
        





# the following is to test
index_obj=Index('all');
index_obj.insert_index_entry('a',4,1)
#test()

        
