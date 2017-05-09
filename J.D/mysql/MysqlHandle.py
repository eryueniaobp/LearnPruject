# -*- coding: utf-8 -*-
import MySQLdb
import datetime
import time
class MysqlHandle:

    def __init__(self):
        self.mysql_conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd="root", db="dj", charset="utf8")
        self.cursor = self.mysql_conn.cursor()

    # 读取前三天action 表中数据
    def readAction(self):
        sql = "select * from action_201602 where '2016-02-03 00:00:00'>time and time>='2016-01-31 00:00:00'"
        self.cursor.execute(sql)
        get_data = self.cursor.fetchall()
        return get_data

    #读取后五天是否购买数据
    def readFiveAction(self,user_id,sku_id):
        sql = "select * from action_201602 where '2016-02-03 00:00:00'<=time and time<'2016-02-08 00:00:00' and user_id="+str(user_id)+" and sku_id="+str(sku_id)+" and type=4"
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        if len(data) == 0:
            return -1
        else:
            return 1

    # 组装样例
    def get_UserProduct(self):
        action_data = self.readAction()
        print "begin"
        for l in action_data:
            user_id = l[1] #用户id
            sku_id = l[2] #商品id
            type_1 = 0  #浏览
            type_2 = 0  #加入购物车
            type_3 = 0  #购物车删除
            type_4 = 0  #下单
            type_5 = 0  #关注
            type_6 = 0  #点击
            action_time = time.mktime(time.strptime(str(l[3]), '%Y-%m-%d %H:%M:%S')) # 用户操作时间
            # 点击模块编号 -1 为null
            if l[4] == "":
                model_id = -1
            else:
                model_id = l[4]

            if l[5] == 1:
                type_1 = 1
            elif l[5] == 2:
                type_2 = 1
            elif l[5] == 3:
                type_3 = 1
            elif l[5] == 4:
                type_4 = 1
            elif l[5] == 5:
                type_5 = 1
            elif l[5] == 6:
                type_6 = 1

            cate = l[6] #品类id
            brand = l[7]    #品牌id

# 获取对应用户数据
            sql_u = "select * from user where user_id="+str(l[1])
            self.cursor.execute(sql_u)
            data_u = self.cursor.fetchall()[0]

            age = self.getage(data_u[2]) #年龄
            sex_1 = 0   #性别 男
            sex_2 = 0   #性别 女
            sex_3 = 0   #性别 保密
            if data_u[3] == 0:
                sex_1 = 1
            elif data_u[3] == 1:
                sex_2 = 1
            elif data_u[3] == 2:
                sex_3 = 1

            user_lv = data_u[4] #用户等级
            user_reg = time.mktime(time.strptime(str(data_u[5]), '%Y-%m-%d'))   #用户注册时间戳

# 获取对应商品信息
            sql_p = "select * from product where sku_id="+str(l[2])
            self.cursor.execute(sql_p)
            data_p = self.cursor.fetchall()

            a1 = 0 #产品属性1
            a2 = 0  #产品属性2
            a3 = 0  #产品属性3
            cate_p = 0 #产品表品类
            brand_p = 0 #产品表品牌
            if len(data_p) != 0:
                a1 = data_p[0][2]
                a2 = data_p[0][3]
                a3 = data_p[0][4]
                cate_p = data_p[0][5]
                brand_p = data_p[0][6]

# 获取对应评论信息
            sql_m = "select * from comment where sku_id="+str(l[2])
            self.cursor.execute(sql_m)
            data_m = self.cursor.fetchall()
            dt = 0  #评论截止时间
            comment_num = -1    #评论累计数分段
            has_bad_comment = -1    #评论是否有差评
            bad_comment_rate = -1   #差评率
            if len(data_m) != 0:
                dt = time.mktime(time.strptime(str(data_m[0][1]),'%Y-%m-%d'))
                comment_num = data_m[0][3]
                has_bad_comment = data_m[0][4]
                bad_comment_rate = data_m[0][5]

#五天以后是否购买
            state = self.readFiveAction(user_id,sku_id)

# 拼接样品数据
            data_str = str(state)+','+str(user_id)+','+str(sku_id)+','+str(type_1)+','+str(type_2)+','+str(type_3)+','+str(type_4)+','+str(type_5)+','+str(type_6)+','+str(action_time)+','+str(model_id)+','+str(cate)+','+str(brand)+','+str(age)+','+str(sex_1)+','+str(sex_2)+','+str(sex_3)+','+str(user_lv)+','+str(user_reg)+','+str(a1)+','+str(a2)+','+str(a3)+','+str(cate_p)+','+str(brand_p)+','+str(dt)+','+str(comment_num)+','+str(has_bad_comment)+','+str(bad_comment_rate)+'\n'
            print data_str
            with open("2016-02-02.csv","a") as l:
                l.write(data_str)

    # 计算天数
    def timeSub(self,now_time,last_time):
        return abs((time.mktime(time.strptime(now_time, '%Y-%m-%d')) - time.mktime(time.strptime(last_time, '%Y-%m-%d'))) / 86400)

    # 设置年龄区段
    def getage(self,str):
        if str == -1:
            return -1
        else:
            if "16" in str:
                return 1
            elif "26" in str:
                return 2
            elif "36" in str:
                return 3
            elif "46" in str:
                return 4
            elif "56" in str:
                return 5

    # 获取mysql 表中字段最大值
    def getMaxDate(self,tableName,field):
        sql = "select max("+field+") from "+tableName
        self.cursor.execute(sql)
        get_data = self.cursor.fetchall()
        return get_data[0][0]

    # 获取mysql 表中字段最小值
    def getMinDate(self,tableName,field):
        sql = "select min("+field+") from "+tableName
        self.cursor.execute(sql)
        get_data = self.cursor.fetchall()
        return get_data[0][0]

    def test(self):
        sql = "select * from action_201602 where id=2"
        self.cursor.execute(sql)
        data = self.cursor.fetchall()[0]
        print str(data[3])
        # print time.mktime(time.strptime(data[3],'%Y-%m-%d %H:%M:%S'))

if __name__=='__main__':
    Obj = MysqlHandle()
    # print Obj.getMinDate('Action_201602','time')
    Obj.get_UserProduct()
    # Obj.test()
    # print Obj.readFiveAction('266079','138778')