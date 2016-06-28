#!/usr/bin/env python2.7.6
# -*- coding:utf-8 -*-
#
# by :darkz
#
#

__update_time_ = "2015-07-30 10:52:40 "
__author__ = "darkz <darkz1984@gmail.com>"


import MySQLdb
import re
import datetime

def get_now_time():
    # return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+".000"
    # 毫秒支持
    return str(datetime.datetime.now())[0:23]

class mysql(object):
    """myslq 对象封装"""
    def __init__(self, my_host, my_user, my_pass='', my_db='information_schema', my_port=3306):
        # def connect(self,my_host, my_user, my_pass='',my_db='information_schema', my_port=3306):
        """连接到mysql"""
        """args:"""

        global int_my_port
        if  not isinstance(my_port, int):
            int_my_port = int(my_port)
        else:
            int_my_port = my_port

        self.conn = MySQLdb.connect(
            host=my_host,
            port=int_my_port,
            user=my_user,
            passwd=my_pass,
            db=my_db)
        print ("connect to %s:%s [%s] [%s]") % ( my_host, my_port, int_my_port, self.conn.port)

        # import pprint
        #　pprint.pprint(dir(self.conn))
        # print(self.conn.port)

    def execute_sql(self, query):
        self.cursor = self.conn.cursor()
        self.cursor.execute('set names utf8')
        try:
            self.cursor.execute(query)
        except BaseException, e:
            print("#############################################")
            print("run %s error!!!!") % (query)
            print("#############################################")
            print("error message is:\n %s !!!!") % (e)
            print("#############################################")
            raise

        self.result = ''  # 保存结果集
        # 正则表达式判断语句是哪类语句
        # 如果是有数据返回的查询
        self.effet_rows = 0
        self.headers = []
        sql_match = re.search(r'^\s*(select|show|desc).*', query, re.I)
        if sql_match:
            #print query
            query_result = self.cursor.fetchall()

            header = self.cursor.description
            self.headers = []

            for header_member in header:
                self.headers.append(str(header_member[0]))
            #print self.headers

            self.effet_rows = self.cursor.rowcount
            #print "%d rows effect" % (self.cursor.rowcount)

            self.result = query_result
            # 返回的结果是元组，转化成二维字符串数组
            # self.result=list(map(map(str,),query_result))
            #self.result = []
            #for row_list in query_result:
            #    # self.result.append(list(map(str,row_list)))
            #    self.result.append(row_list)
            #print ("%s") % ('execute query over')
        else:
            # 提交事务，否则数据修改操作无法保存
            self.conn.commit()
            #return ("execute query [%s]...") % ('query')
        result = mysql_result(self.headers, self.result, self.effet_rows)
        return result

    def write_array_2_mysql(self, table_to_write, mysql_result_data, lines_per_time=1000, write_mode='insert'):
        """write a list to mysql tables
        large list can be split small ones to write many times
        """
        write_query = ''
        if mysql_result_data.headers:
            col_name_list = ",".join(mysql_result_data.headers)
            write_query = """$insert_type into $table_name ($col_name_list) values ($col_list)"""
            write_query = write_query.replace("$col_name_list", col_name_list )
        else:
            write_query = """$insert_type into $table_name values ($col_list)"""

        col_list = ','.join(['%s'] * len(mysql_result_data.data[0]))

        write_query = write_query.replace("$insert_type", write_mode )
        write_query = write_query.replace("$table_name", table_to_write )
        write_query = write_query.replace("$col_list", col_list )

        # self.cursor.executemany(write_query, mysql_result_data.data)
        # self.conn.commit()

        # 将一次大量数据写入分隔成小批量写入
        list_shift = 0
        while list_shift < len(mysql_result_data.data):
            # 将较大数据切割成小数组多次写入,原始数组还是保留
            print get_now_time()
            data_list_slice = mysql_result_data.data[list_shift: list_shift + lines_per_time]
            list_shift = list_shift + lines_per_time
            self.cursor.executemany(write_query, data_list_slice)
            self.conn.commit()

    def write_array_2_mysql(
            self, table_to_write, data_list, lines_per_time=1000, headers=''):
        """write a list to mysql tables
        large list can be split small ones to write many times
        """
        # print "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        # print(dir(data_list))
        print len(data_list)
        sql_head = ''
        if headers:
            sql_head = ",".join(headers)
            sql_head = ('replace into `%s` (%s) values ') % (table_to_write, sql_head)
            # print type(sql_head)
            # print sql_head
        else:
            # sql_head = ('insert into `%s` values ') % (table_to_write)
            sql_head = ('replace into `%s` values ') % (table_to_write)
            # print sql_head

        list_shift = 0
        while list_shift < len(data_list):
            # 将较大数据切割成小数组多次写入,原始数组还是保留
            # 将数组转化成元组后再转化成字符串就没有了数字的方括号转化问题
            data_list_slice = tuple(
                map(
                    tuple,
                    (data_list[list_shift: list_shift + lines_per_time]))
            )
            # 注意str将数组转化成字符串
            sql_data = ','.join(map(str, data_list_slice))
            list_shift = list_shift + lines_per_time  # 偏移量修改
            sql = ('%s %s ;') % (sql_head, sql_data)
            print "1111111111111111111111111111111111111"
            print sql
            self.execute_sql(sql)



class mysql_result(object):
    """
    the mysql result object
    contain the headers(columns name list)
    data a data string list
    effect_rows data list rows count
    """
    def __init__(self, headers, data, effet_rows):
        self.headers = headers
        self.data = data
        self.effet_rows = effet_rows

if __name__ == '__main__':

    def get_now_time():
        # return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+".000"
        # 毫秒支持
        return str(datetime.datetime.now())[0:23]

    mysql_conn = mysql('127.0.0.1', 'root', '', 'game_data_sys', my_port='3316')

    result = mysql_conn.execute_sql("select * from tab_sys_charge_order where create_time>='2015-12-01' limit 1;")
    print result.data

    print "%s" % 'over'
    print type(result)

    import random
    import pprint

    # 测试写入及特殊符号处理、
    sample = ['[', ']', ',', "'", '6', '7', '8', '9', '10', 'a', 'b', 'c', 'd', 'e']
    sample_num = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    # sample = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'a' ,'b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
    list = []
    max = 10
    print get_now_time()
    for i in range(0, max):
        # print i
        # if i % (max/100*2)==0:
        #    print i
        # list.append[rand.rand(),rand.rand]
        name1 = "".join(random.sample(sample, 5))
        value1 = "".join(random.sample(sample, 10))
        id = "".join(random.sample(sample_num, 2))
        # print name1
        # print value1
        list.append([id, name1, value1])
    pprint.pprint(list)

    print get_now_time()
    #for i in range(1000):
    #    print get_now_time()
    #    mysql_conn.write_array_2_mysql('region_login_copy', result ,lines_per_time=100)

    print get_now_time()
    # raw_input()
