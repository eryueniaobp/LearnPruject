# -*- coding: utf-8 -*-
# import pandas

# u_object = pandas.read_csv("./JData/JData_Action_201602.csv",encoding='gbk')
# first_one = u_object.values
# for l in first_one:
#     for v in l:
#         print type(v)
#     exit()
with open("./JData/JData_Action_201603.csv","rb") as l:
    for i in l:
        print type(i.strip())
        exit()