#!/usr/bin/python
# -*- coding:utf-8 -*-
#
# Created on 2014-08-02 17:21:41
#
#
# 文件说明
#
# ====================================================

__author__ = 'darkz'
__version__ = '1.0'
__update_date__ = '2016-06-26 11:34:31 '


import urllib
import urllib2
import cookielib
import time
import os
import pprint
import sys
sys.path.append('./lib')
import db_mysql
reload(sys)
sys.setdefaultencoding('utf-8')
import pprint

class npc(object):
    def __init__(self):
        self.id = 0
        self.url = ''
        self.name_str = ''
        self.namealt_str = ''

    def get_npc_by_id(self, id):
        self.id=id
        self.url= ('http://db.duowan.com/wow/npc-%s.html')%(self.id)
        data = pq(self.url)
        LogTitle = data('h3.title')  #  tag class
        for i in LogTitle:
            #self.name_str = pq(i).text().encode('UTF-8')
            self.name_str = pq(i).text()



if __name__ == '__main__':


    mysql_conn = db_mysql.mysql('192.168.18.171', 'darkz', 'iBdfHESB7FRW3eNm2tbx', 'mysql', my_port='3306')
    mysql_conn.execute_sql("set names utf8")
    mysql_conn.execute_sql("select id from world.quest_template where id>=1479")
    mysql_conn.result=[[54]]

    from pyquery import PyQuery as pq


    file=open('data_npc','a')
    #print dir(mysql_conn)
    for line in mysql_conn.result:

        npc_id = line[0]
        print npc_id
        npc1 = npc()
        npc1.get_npc_by_id(npc_id)
        pprint.pprint(vars(npc1))
        print type(npc1.name_str)

        line="||||".join([str(npc1.id), npc1.name_str, npc1.namealt_str])
        line=("%s\n")%(line)

        file.write(line)
    file.close()
