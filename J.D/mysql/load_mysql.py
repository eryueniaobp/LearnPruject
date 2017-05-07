import pandas
import MySQLdb
class load_mysql(object):
    def __init__(self,filename=""):
        self.filename = filename
        self.mysql_conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd="root", db="dj", charset="utf8")
        self.cursor = self.mysql_conn.cursor()
        if self.filename != "":
            self.file_Object = pandas.read_csv("../JData/JData_"+self.filename+".csv",encoding='gbk')

    def load_data(self):
        column = self.file_Object.columns
        content = self.file_Object.values
        column_str = ""
        for i in column:
            column_str += i.encode('utf8')+','
        column_str = column_str[:-1]
        for l in content:
            value_str = ""
            for v in l:
                if pandas.isnull(v):
                    value_str += "NULL"+","
                else:
                    if type(v) == long:
                        value_str += "'"+str(int(v))+"'"+','
                    elif type(v) == unicode:
                        value_str += "'"+v.encode('utf8')+"'"+','
                    elif type(v) == float:
                        value_str += "'"+str(v)+"'"+','
                    # value_str += "'"+str(v)+"'"+','
            value_str = value_str[:-1]
            sql = "insert into "+self.filename+" ("+column_str+")VALUES("+value_str+")"
            print sql
            self.cursor.execute(sql)
        self.mysql_conn.commit()
    def del_data(self,name):
        sql = "truncate table "+name
        self.cursor.execute(sql)
        self.mysql_conn.commit()

    def __del__(self):
        self.mysql_conn.close()

if __name__ == '__main__':
    object = load_mysql()
    object.del_data('action_201603')