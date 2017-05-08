# -*- coding: utf-8 -*-
import MySQLdb
import datetime
class MysqlHandle:

    def __init__(self):
        self.mysql_conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd="root", db="dj", charset="utf8")
        self.cursor = self.mysql_conn.cursor()

    def readMysql(self):
        sql = "select * from action_201602 where '2016-02-02 23:59:59'>=time and time>='2016-01-31 00:00:00'"
        get_rows = self.cursor.execute(sql)
        get_data = self.cursor.fetchall()
        return get_data
    def get_UserProduct(self):
        action_data = self.readMysql()
        for l in action_data:
            sql_u = "select * from user where user_id="+str(l[1])
            self.cursor.execute(sql_u)
            data_u = self.cursor.fetchall()[0][2:]

            sql_p = "select * from product where sku_id="+str(l[2])
            self.cursor.execute(sql_p)
            data_p = self.cursor.fetchall()[0][2:]

            sql_m = "select * from comment where sku_id="+str(l[2])
            self.cursor.execute(sql_m)
            data_m = self.cursor.fetchall()[0][1:]

            data = l+data_u+data_p+data_m
            print data
            exit()

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
        sql = "select * from user where id=50"
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        for l in data:
            print l

if __name__=='__main__':
    Obj = MysqlHandle()
    # print Obj.getMinDate('Action_201602','time')
    Obj.get_UserProduct()
