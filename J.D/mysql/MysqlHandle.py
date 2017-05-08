# -*- coding: utf-8 -*-
import MySQLdb
import datetime
class MysqlHandle:

    def __init__(self):
        self.mysql_conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd="root", db="dj", charset="utf8")
        self.cursor = self.mysql_conn.cursor()

    def readMysql(self):
        sql = "select * from action_201602 where  '2016-02-02'>=time and time>='2016-02-01'"
        get_rows = self.cursor.execute(sql)
        get_data = self.cursor.fetchall()
        print get_data

    def getMaxDate(self,tableName,field):
        sql = "select max("+field+") from "+tableName
        self.cursor.execute(sql)
        get_data = self.cursor.fetchall()
        return get_data[0][0]

    def getMinDate(self,tableName,field):
        sql = "select min("+field+") from "+tableName
        self.cursor.execute(sql)
        get_data = self.cursor.fetchall()
        return get_data[0][0]

    def test(self):
        sql = "select * from user where id=2"
        self.cursor.execute(sql)
        print str(self.cursor.fetchall()[0])
if __name__=='__main__':
    Obj = MysqlHandle()
    # print Obj.getMinDate('Action_201602','time')
    # Obj.readMysql()
    Obj.test()