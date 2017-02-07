#!/usr/bin/python
# -*- coding:utf8 -*-
# @author: holmer
# @email: 1107683874@qq.com
# @version 1.0

import time,os,sys,re
import MySQLdb
import getopt

version = "1.0"
input_values = {'file':None,'outfile':None,'mode':'normal','database':'test','table':None,'user':None,'password':None,'port':None,'host':'localhost','socket':'/tmp/mysql_3306.sock','ddl':'no','dml':'yes','s_id':'no'}
schema_dicts = {}
reset_schema_dicts = False

"""
	将提示报错等信息以红色字体颜色打印出来
"""
def print_red(print_str):
	print "\033[1;31;40m",
	print print_str
	print "\033[0m",

"""
	帮助函数，显示帮助信息
"""			
def usage():
	print("Usage: ")
	print("  python binlog_parse.py -uholmer -pholmer -h192.168.247.202 -P3306 -Btest -f d123.txt -o binlog_output_convert.txt -m convert -d yes -i yes -M no")
	print("  --------------------------------------------------------------------------------------------------------------------------------------------- ")
	print("  -f, --file             Parse file. eg: mysqlbinlog -vvv /disk2/holmer/master-bin.000017 > /disk2/holmer/d123.txt ")
	print("  -o, --outfile          Output file. ")
	print("  -m, --mode             If equal to normal, generate sql; if equal to convert, generate backing sql(eg: insert->delete; delete->insert;). Only applicable to dml. default: normal. ")
	print("  -B, --database         Schema name. Isn't used to filter the database. Just used to connect to the server. ")
	print("  -t, --table            Table name. Only get this tables, multi tables split by ',', do not support regex. eg: test.tb1,db_name.tb_name")
	print("  -u, --user             Connect to the server as username. ")
	print("  -p, --password         Password to connect to remote server. ")
	print("  -P, --port             Port number to use for connection. ")
	print("  -h, --host             Connect to host. ")
	print("  -S, --socket           The socket file to use for connection. ")
	print("  -M, --dml              Get dml content. default: yes; If equal to no, will not get dml content. ")
	print("  -d, --ddl              Get ddl content. default: no; If equal to yes, will get ddl content. ")
	print("  -i, --s_id             Get server id lines. default: no; If equal to yes, will get server id lines. ")
	print("  -H, --help             Usage. ")
	print("  -v, --version          Print version. ")

"""
	对传入的参数进行过滤和判断
	@para 	sys_argv 			list
"""
def parameter_filter(sys_argv):
	opts=None
	args=None
	try:
		opts,args = getopt.getopt(sys_argv[1:], "Hf:o:vm:B:t:u:p:P:h:S:M:d:i:", ["help","file=","outfile=","version","mode=","database=","table=","user=","password=","port=","host=","socket=","ddl=","dml=","s_id="])
	except getopt.GetoptError,ex:
		print_red("  Invalid options! " + ex[0].split('-')[0].capitalize() + '-' + ex[0].split('-')[1] + "!")
		usage()
		sys.exit(2)

	for opt, arg in opts:
		if opt in ("-H", "--help"):
			usage()
			sys.exit()
		elif opt in ("-v", "--version"):
			print(version)
			sys.exit()
		elif opt in ("-f", "--file"):
			input_values['file'] = arg
		elif opt in ("-o", "--outfile"):
			input_values['outfile'] = arg
		elif opt in ("-m", "--mode"):
			input_values['mode'] = arg
		elif opt in ("-B", "--database"):
			input_values['database'] = arg
		elif opt in ("-t", "--table"):
			input_values['table'] = arg
		elif opt in ("-u", "--user"):
			input_values['user'] = arg
		elif opt in ("-p", "--password"):
			input_values['password'] = arg
		elif opt in ("-P", "--port"):
			input_values['port'] = arg
		elif opt in ("-h", "--host"):
			input_values['host'] = arg
		elif opt in ("-S", "--socket"):
			input_values['socket'] = arg
		elif opt in ("-M", "--dml"):
			input_values['dml'] = arg
		elif opt in ("-d", "--ddl"):
			input_values['ddl'] = arg
		elif opt in ("-i", "--s_id"):
			input_values['s_id'] = arg
		else:
			usage()
			sys.exit(2)

	if None in [input_values['user'], input_values['password'], input_values['port'], input_values['file'], input_values['outfile']]:
		print_red(" Please check your user, password and port ! Or must haved input/output file! ")
		usage()
		sys.exit(2)
	
	
"""
	MySQL 数据库操作类
"""
class db_operation:
	def __init__(self):
		self._conn_flag = True
		self._database = input_values['database']
		self._user = input_values['user']
		self._password = input_values['password']
		self._port = int(input_values['port'])
		self._host = input_values['host']
		self._socket = input_values['socket']
		self._charset = 'utf8'
		self._conn = self.connect_mysql()

		if(self._conn):
			self._cursor = self._conn.cursor()

	def connect_mysql(self):
		try:
			conn = MySQLdb.connect(host = self._host,
					user = self._user,
					passwd = self._password,
					db = self._database,
					port = self._port,
					charset = self._charset,
					unix_socket = self._socket
				)
		except Exception, ex:
			print_red("Connect database failed, %s" % ex)
			self._conn_flag = False
			sys.exit(1)
		return conn
	
	def fetch_all(self, sql):
		res = ''
		if(self._conn):
			try:
				self._cursor.execute(sql)
				res = self._cursor.fetchall()
			except Exception, ex:
				res = False
				print_red("query database exception, %s" % ex)
		return res
	
	def __del__(self):
		if(self._conn_flag):
			self._cursor.close()
			self._conn.close()


"""
	获取表名, 当前读取的行存在 Table_map 字符时, 从该行中获取表名和数据库名
	@para 	line 			string
	@return schema_table 	dict
"""	
def get_table_name(line):
	try:
		cha = line.index('Table_map')
		schema = line[cha::].split(' ')[1]
		cha_list = schema.replace('`','').split('.')
		if reset_schema_dicts:
			global schema_dicts
			schema_dicts = {}
		if schema in schema_dicts:
			return 1
		else:
			db_name = cha_list[0]
			tb_name = cha_list[1]
			schema_table = {'schema_name':db_name, 'table_name':tb_name}
			schema_dicts[schema] = schema_table
			return schema_table
	except Exception, ex:
		print_red("Get table name failure! %s" % ex)

"""
	获取表的字段, 根据传入的参数, 获取该表的字段
	@para 	schema_table	dict
"""	
def get_table_columns(schema_table):
	try:
		conn = db_operation()
		res = conn.fetch_all("select concat('`', table_schema, '`.`', table_name, '`') db_tb, count(1) as column_count, group_concat(column_name order by ordinal_position) table_columns from information_schema.columns where table_schema='%s' and table_name='%s' limit 1" % (schema_table['schema_name'], schema_table['table_name']))
		for row in res:
			if row[0] == None:
				print_red("table: %s not in schema: %s !" % (schema_table['table_name'], schema_table['schema_name']))
				sys.exit(1)
			table_dict = {'column_count':row[1], 'table_columns':row[2]}
			schema_tb = row[0]
			schema_dicts[schema_tb] = table_dict
	except Exception, ex:
		print_red("Get table columns failure! %s" % ex)
		sys.exit(1)

"""
	binlog 解析函数(字符串查找find方法比search方法效率高很多, 能不用正则的情况下尽量不用)
	update要注意这种写法: update tb1 a inner join tb2 b on a.id=b.id set a.name='test123', b.name='test456' where a.id=6;
	delete要注意这种写法: delete a.*, b.* from tb1 a inner join tb2 b on a.id=b.id where a.id in (1,2,3,4,5);
	读写文件可以用 codecs 包(不考虑效率的情况下, 在本例子中, 用 codecs 包读取250M的文件性能损耗下降差不都50%), 尽量使用原生的方法
"""
def parse_binlog_file():
	with open(input_values['file'], 'rt') as read_handle:
		with open(input_values['outfile'], 'w+') as write_handle:
			table_filter_flag = True
			ddl_start_flag = False
			ddl_end_flag = True
			if None in [input_values['table']]:
				filter_tables = None
			else:
				filter_tables = input_values['table'].split(',')
			
			table_rows = ''
			real_columns = None
			vm_column_count = 1
			i = 1
			
			for line in read_handle:
				if input_values['dml'] == 'yes':
					if line.find('### ', 0, 4) != -1:
						if i == 1:
							if table_filter_flag:
								if line.find('###   @') != -1:
									print_red("Table: %s structure has changed ! less a few columns ! " % cur_table_name)
									sys.exit(2)
							else:
								if line.find('### DELETE FROM') != -1 or line.find('### UPDATE ') != -1 or line.find('### INSERT INTO ') != -1:
									table_filter_flag = True
								else:
									continue
							cur_table_name = line.split(' ')[-1].replace('\n','')
							if filter_tables is not None and cur_table_name.replace('`', '') not in filter_tables:
								table_filter_flag = False
							real_column_count = schema_dicts[cur_table_name]['column_count']
							real_columns = schema_dicts[cur_table_name]['table_columns'].split(',')
							table_rows = re.sub('### ', '', line)
							if line.find('### DELETE FROM') != -1 or line.find('### INSERT INTO ') != -1:
								vm_column_count = real_column_count + 2
							if line.find('### UPDATE ') != -1:
								vm_column_count = real_column_count * 2 + 3
						
						if table_filter_flag and line.find('###   @') != -1:
							try:
								line = unicode(line, 'utf-8')
							except Exception, ex:
								print_red("binlog parse failure! %s" % ex)
								#print_red("current table: %s; current line: %s" % (cur_table_name, line))
								sys.exit(1)
							column_num = line.replace('###   @', '').split('=')[0]
							row_column = line.replace('###   @' + str(column_num), '`' + real_columns[int(column_num)-1] + '`')
							row_column = re.sub('\/\*.*', '', row_column)
							row_column = row_column.strip()
							if i == vm_column_count:
								table_rows += row_column + ';'
							else:
								table_rows += row_column + ', '
						elif table_filter_flag and i != 1:
							if line.find('### DELETE FROM') != -1 or line.find('### UPDATE ') != -1 or line.find('### INSERT INTO ') != -1:
								print_red("Table: %s structure has changed ! more than a few columns ! " % cur_table_name)
								sys.exit(2)
							table_rows += re.sub('### ', '', line)
						elif table_filter_flag == False:
							i = 1
							continue
						
						i += 1
						if i > vm_column_count:
							i = 1
							if table_filter_flag:
								table_rows = table_rows.replace('\n', ' ')
								if table_rows.find('DELETE FROM', 0, 11) != -1:
									if input_values['mode'] == 'normal':
										table_rows = table_rows.replace(', ', ' and ')
									else:
										table_rows = table_rows.replace('DELETE FROM', 'INSERT INTO').replace(' WHERE ', ' SET ')
								elif table_rows.find('UPDATE ', 0, 7) != -1:
									table_rows_re = re.search('UPDATE (.*`) WHERE(.*) SET(.*)', table_rows)
									if input_values['mode'] == 'normal':
										table_rows = 'UPDATE ' + table_rows_re.group(1) + ' SET ' + table_rows_re.group(3).replace(';', ' ') + ' WHERE ' + table_rows_re.group(2).replace(', ', ' and ')[::1].replace(',', ';', 1)
									else:
										table_rows = 'UPDATE ' + table_rows_re.group(1) + ' SET ' + table_rows_re.group(2)[::-1].replace(',', '', 1)[::-1] + ' WHERE ' + table_rows_re.group(3).replace(', ', ' and ')
								else:
									if input_values['mode'] != 'normal':
										table_rows = table_rows.replace('INSERT INTO', 'DELETE FROM').replace(' SET ', ' WHERE ').replace(', ', ' and ')
									
								write_handle.write(table_rows.encode('utf-8') + '\n')
							table_filter_flag = True
						continue
					
					if line.find('Table_map: ') != -1:
						schema_table = get_table_name(line)
						if schema_table <> 1:
							get_table_columns(schema_table)
						write_handle.write(line)
						continue
				
				if input_values['s_id'] == 'yes':
					if line.find('server id') != -1:
						write_handle.write(line)
						continue
				
				if input_values['ddl'] == 'yes':
					#ddl_match = re.match(r'create |alter |drop |rename |truncate ', line, re.I)
					lower_line = line.lower()
					if lower_line.find('create ', 0, 7) != -1:
						ddl_match = 1
					elif lower_line.find('alter ', 0, 6) != -1:
						reset_schema_dicts = True
						ddl_match = 1
					elif lower_line.find('drop ', 0, 5) != -1:
						ddl_match = 1
					elif lower_line.find('rename ', 0, 7) != -1:
						ddl_match = 1
					elif lower_line.find('truncate ', 0, 9) != -1:
						ddl_match = 1
					else:
						ddl_match = -1
					
					#ddl_end_match = re.match(r'/\*\!\*/;', line)
					ddl_end_match = line.find('/*!*/;', 0, 6)
					
					#if ddl_match:
					if ddl_match != -1:
						ddl_start_flag = True
					
					if ddl_start_flag and ddl_end_flag:
						write_handle.write(line)
					
					if ddl_end_match != -1 and ddl_start_flag:
						ddl_end_flag = False
					
					if ddl_end_match != -1 and ddl_start_flag:
						ddl_start_flag = False
						ddl_end_flag = True
					
					use_match = lower_line.find('use ', 0, 4)
					if use_match != -1:
						write_handle.write(line)

"""
	入口函数
	调用例子: 
		cp /disk2/mysql_3306/binlog/master-bin.000017 /disk2/holmer/
		mysqlbinlog -vvv /disk2/holmer/master-bin.000017 > /disk2/holmer/d123.txt 
		time python binlog_parse.py  -uholmer -pholmer -h192.168.247.202 -P3306 -Btest -f d123.txt -o binlog_output.txt -d yes -i yes
		python binlog_parse.py -uholmer -pholmer0319 -S /tmp/mysql_3325.sock -P3325 -f 000413.txt -t bpms.sale_shopping_cart_item -B bpms  -i yes -m normal -o binlog_out_normal.sql
		如果编码报错, 则将 /disk2/holmer/d123.txt 文件进行编码转换或进行输出编码转换：
			查看文件编码: vim /disk2/holmer/d123.txt, 然后输入 :set fileencoding 就可以查看文件编码了(假设这里是latin1)
			方法一: 2.1 将 latin1 编码转换成 utf-8 编码: iconv -f latin1 -t utf-8 /disk2/holmer/d123.txt > /disk2/holmer/utf8_d123.txt
					2.2 最后将输出文件转为原先的格式: iconv -f utf-8 -t latin1 binlog_output.txt > latin1_binlog_output.txt
			方法二: 3.1	修改 parse_binlog_file 函数中的 line = unicode(line, "utf-8"), 将 utf-8 改为 latin1
					3.2 修改 parse_binlog_file 函数中的 write_handle.write(table_rows.encode('utf-8') + '\n'), 将 utf-8 改为 latin1
		总之, 在对文件进行操作前, 最好先对文件进行 utf-8 转码
	单步调试方法: 
		python -m pdb binlog_parse.py -uholmer -pholmer -h192.168.247.202 -P3306 -Btest -f d123.txt -o binlog_output.txt -d yes -i yes
"""		
if	__name__=='__main__':

	if(len(sys.argv) <= 1):
		usage()
		sys.exit(2)
	
	parameter_filter(sys.argv)
	parse_binlog_file()
