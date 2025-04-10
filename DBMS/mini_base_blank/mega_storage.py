#------------------------------------------------
# mega_stroage.py
# author: Jingyu Han, hjymail@163.com
# modified by:Shuting Guo, shutingnjupt@gmail.com
#------------------------------------------------

'''
mega_storage.py is to store table data in separate files.
Each table is stored in a separate file with the suffix ".dat".
For example, table named moviestar is stored in file moviestar.dat.
As it is to demonstrate principles in chapter one, it is rather simple.
The file is in ASCII text format, not binary one.
Each line corresponds to one record and different field values are separated by |
'''

import os

#--------------------------------------------
# the class can store table data into files
# functions include insert, delete and update
#--------------------------------------------

class MegaStorage(object):

	# ------------------------------
	# constructor of the class
	# input:
	#       tablename
	# -------------------------------------
	def __init__(self, tablename):  # each table coresponds to one file with suffix .dat
		print ("__init__ of ", MegaStorage.__name__)
		tablename.strip()
		self.record_list = []  # a main memory list to store all the records

		if not os.path.exists(tablename + '.txt'):  # the data file does not exist
			print ('table file ' + tablename + '.txt does not exists')
			self.f_handle = open(tablename + '.txt', 'w+')
			print (tablename + '.txt has been created')
			self.num_of_fields = None

		else:  # the file do exist

			self.f_handle = open(tablename + '.txt', 'r+')  ##a
			print ('table file ' + tablename + '.txt is opened now')

			# show the data in the table( file)
			for each_line in self.f_handle:
				self.record_list.append(each_line.strip())
				# print each_line.strip()


	# ----------------------
	# destruct of class
	# ------------------------
	def __del__(self):
		print ("__del__ of ", MegaStorage.__name__)
		if self.f_handle:
			self.f_handle.close()

	# --------------------------------
	# to insert only one record into table, the values are input by users
	# input
	#   field_name_list: the names of all fields
	# -------------------------------
	def insert_record(self, field_name_list):

		if len(field_name_list) > 0:  # schema is provided

			record_str = ''
			for i in range(len(field_name_list)):
				temp_value = input('pleas insert the field value of ' + field_name_list[i] + ' :')
				record_str = record_str + temp_value.strip() + '|'

			temp_len = len(record_str)
			last_str = record_str[0:temp_len - 1]

			print ('value is', last_str)
			self.record_list.append(last_str)
			self.f_handle.write(last_str + '\n')
			self.f_handle.flush()


		else:  # there is no schema given
			print ('wrong in insert_record for the schema is not given')

	# --------------------------------
	# to view all records in the table
	# input
	# -------------------------------
	def view_all(self):
		if len(self.record_list) > 0:


			for i in range(len(self.record_list)):
				print (self.record_list[i])

	# --------------------------------
	# to delete one record from the table
	# input
	#       value_list: the list of field values of which each element is a tuple (field_name,new_field_value)
	# Author: Shuting Guo
	# -------------------------------
	def del_one_record(self, value_list,field_name_list):
		updateIndex = field_name_list.index(value_list[0])
		tmp_List=[]
		for record in self.record_list:
			if record.split('|')[updateIndex] != value_list[1]:
				tmp_List.append(record)
		self.record_list=tmp_List[:]

		self.f_handle.seek(0)        #back to the head of file,offset=0
		self.f_handle.truncate(0)    #cut all data after offset=0
		# print self.f_handle.tell()
		self.f_handle.write('\n'.join(self.record_list))  #write record_list
		self.f_handle.write('\n')
		self.f_handle.flush()


	# -------------------------------
	# delete all records from the table
	# ---------------------------------
	def delete_table_data(self):
		self.f_handle.truncate(0)
		self.f_handle.seek(0)
		self.f_handle.flush()

	#--------------------------
	# to delete the data file
	#--------------------------
	def delete_data_file(self,tablename):
		if os.path.exists(tablename+'.txt'):
			self.f_handle.close()
			os.remove(tablename+'.txt')

	# --------------------------------
	# to update one record of the table
	# input
	#       condition_list: the where conditon, of which each element is a tuple (field_name, field_value)
	#       new_value_list: new value list, of which each element is a tuple (field_name,new_field_value)
	# -------------------------------

	def update_record(self, condition_list, new_value_list,field_name_list):
		updateIndex = field_name_list.index(condition_list[0])
		for idx in range(len(self.record_list)):
			tmp = self.record_list[idx].split('|')
			if tmp[updateIndex] == condition_list[1]:
				tmp[updateIndex] = new_value_list[1]
				self.record_list[idx] = '|'.join(tmp)

		self.f_handle.truncate(0)
		#print self.f_handle.tell()
		self.f_handle.seek(0)
		self.f_handle.write('\n'.join(self.record_list))
		self.f_handle.write('\n')
		self.f_handle.flush()