#encoding=utf-8
import pandas as pd
import  numpy as np
import datetime,sys,subprocess,random
import matplotlib.pyplot as plt

from sklearn.linear_model import LogisticRegression
from sklearn.datasets import dump_svmlight_file

from sklearn.metrics import  roc_auc_score , f1_score,fbeta_score

root = '/Users/baipeng/PycharmProjects/try/JData/'
user_csv = '{root}/JData_User.csv'.format(root=root)
prod_csv = '{root}/JData_Product.csv'.format(root=root)


comment_csv = '{root}/JData_Comment.csv'.format(root=root)
comment_aux_csv = '{root}/JData_Comment_aux.csv'.format(root=root)

action_csv = '{root}/JData_Action.csv'.format(root=root) #全部的action放一起

headact_csv = '{root}/J20.csv'.format(root=root)
act_error_csv = '{root}/Je.csv'.format(root=root)

full_csv = '{root}/full.csv'.format(root=root)
label_aux_csv = '{root}/label_aux.csv'.format(root=root)
label_pos_csv = '{root}/label_pos.csv'.format(root=root)


bingo_csv = '{root}/bingo.csv'.format(root=root)
"""
    dt,sku_id,comment_num,has_bad_comment,bad_comment_rate

"""
comment_dts = '2016-02-01,2016-02-08,2016-02-15,2016-02-22,2016-02-29,2016-03-07,2016-03-14,2016-03-21,2016-03-28,2016-04-04,2016-04-11,2016-04-15'.split(',')

keys = ',user_id,sku_id,time,model_id,type,cate_x,brand_x,age,sex,user_lv_cd,user_reg_tm,a1,a2,a3,cate_y,brand_y'.split(',')
full_keys = ',Y,user_id,sku_id,time,model_id,type,cate_x,brand_x,age,sex,user_lv_cd,user_reg_tm,a1,a2,a3,cate_y,brand_y,comment_num,has_bad_comment,bad_comment_rate'.split(',')



"""
comment 热度
"""
def search_back(target ,ds):
    pre = ds[0]
    for d in ds:
        if target  <=  d:
            return pre
        pre = d
    return pre
def search_fore(target ,ds):

    for d in ds:
        if target  <=  d:
            return d
    return d
def transfer_comment():

    existed =['2016-02-01','2016-02-08','2016-02-15','2016-02-22','2016-02-29','2016-03-07','2016-03-14','2016-03-21','2016-03-28','2016-04-04','2016-04-11','2016-04-15']
    day = datetime.datetime.strptime('2016-02-01','%Y-%m-%d')
    days = []
    for i in range(75):
        d = (day +   datetime.timedelta(days = i )).strftime('%Y-%m-%d')

        days.append(d)


    df = pd.read_csv(comment_csv,dtype=str)

    dfs = [df]
    # with open(comment_aux_csv,'w') as f:
    #     keys = 'dt,sku_id,comment_num,has_bad_comment,bad_comment_rate'
    #     f.write(keys + '\n')

    for d in days:
        if d not in existed:
            # build a  comment
            backday = search_back(d, existed)
            print 'backday ' , d ,backday

            foreday = search_fore(d, existed)

            back_df = df[df['dt'] == backday].copy()
            # fore_df = df[df['dt'] == foreday ].copy()

            back_df['dt']  =  d
            dfs.append(back_df)
            # back_df['c']

    comment_aux_df = pd.concat(dfs)

    comment_aux_df.to_csv(comment_aux_csv)








def transfer_label():
    df = pd.read_csv(label_aux_csv,dtype=str, index_col=0)
    # print df
    c = 0
    with open(label_pos_csv, 'w') as f:
        keys = 'Y,time,user_id,sku_id'
        f.write(keys+'\n')
        for node in df.values:
            print c
            c+=1
            time,user_id ,sku_id = node[0],node[1],node[2]
            # print time ,user_id,sku_id
            day = datetime.datetime.strptime(time[0:10],'%Y-%m-%d')
            for i in range(1,6):
                d = (day -  datetime.timedelta(days = i )).strftime('%Y-%m-%d')
                line = '1,{d},{user_id},{sku_id}'.format(d=d,user_id=user_id,sku_id=sku_id)

                f.write(line+'\n')


"""
Use Pandas to speed.

merge comment and label_aux
"""
def build_sample_df(each_out,each_in):
    comment_df = pd.read_csv(comment_aux_csv,dtype=str,index_col = 0)

    aux_df = pd.read_csv(label_pos_csv,dtype=str)
    print aux_df.head()
    aux_df['time'] = aux_df['time'].str[0:10]


    action_user_prod_df = pd.read_csv(each_in,index_col=0,dtype=str) #partial with index

    action_user_prod_df['time'] = action_user_prod_df['time'].str[0:10] # day

    act_comment_df = pd.merge(action_user_prod_df , comment_df , how= 'left', left_on=['time','sku_id'], right_on=['dt','sku_id'])

    print act_comment_df.columns,act_comment_df.count()
    #with label
    sample_df = pd.merge(act_comment_df , aux_df , how='left' , left_on = ['time', 'user_id','sku_id'] , right_on= ['time', 'user_id','sku_id'])
    # print sample_df.columns ,sample_df.count()
    sample_df['Y'] = sample_df['Y'].map(lambda x: '1' if x == '1' else '-1')
    print sample_df.head()
    sample_df.to_csv(each_out)

def split_action():

    act_user_prod_df = pd.read_csv('out.csv',index_col=0)


    for name , g in act_user_prod_df.groupby( act_user_prod_df['time'].str[0:10] ):
        g.to_csv( './partial/' + name + '.csv')

def concat_sample():
    days  = sys.argv[2].split('_')

    dfs = []
    for d  in days:
        df = pd.read_csv( '{root}/sample2/{day}.csv'.format(
            root=root,day=d
        )
                          ,dtype=str,index_col= 0)

        dfs.append(df)

    cdf  =pd.concat(dfs)

    cdf.to_csv(full_csv)





"""
第一列是 label
后续是  特征
直接用scikit 学习

time不能做特征
"""
def train():

    with open(full_csv,'r') as f:
        for L in f:
            full_keys = L.strip().split(',')
            break




    full_df = pd.read_csv(full_csv, dtype=str )
    #为了统一用 dummies, pd =1 ,用于预测
    # predict X
    ds  = []
    day = datetime.datetime.strptime('2016-04-14', '%Y-%m-%d')
    for i in range(5):
        d = (day + datetime.timedelta(days = i )).strftime('%Y-%m-%d')
        ds.append(d)


    train_days = []
    day = datetime.datetime.strptime('2016-04-01', '%Y-%m-%d')
    for i in range(13):
        d = (day + datetime.timedelta(days = i )).strftime('%Y-%m-%d')
        train_days.append(d)

    full_df['pd'] =  full_df['time'].map ( lambda  x : 1  if x in ds  else 0 )

    # pred_df = full_df[ full_df['pd'] == 1 ]
    #
    # train_df = full_df[ full_df['pd'] == 0 ]
    #
    # neg_train_df = train_df [ train_df['Y'] == '-1' ]
    # part_neg_df = neg_train_df.sample(frac = 0.3,replace= True )
    # pos_train_df = train_df [ train_df['Y'] == '1' ]
    #
    # print 'before sample ' , len(full_df) , ' train = '  ,len(train_df)
    #
    # #todo:   随机打乱 行的顺序
    # full_df = pd.concat( [  part_neg_df , pos_train_df ,pred_df ] )


    train_df = full_df[ full_df['pd'] == 0 ]

    print 'after sample ' ,  len(full_df), 'train = ' , len(train_df)


    full_keys.append('pd')
    fs = []
    i = -1
    for k in full_keys:
        i += 1
        # if i == 0:
        #     continue
        if k in [ 'Y','time','', 'dt', 'user_reg_tm']: #这些不作为特征
            continue
        fs.append( i )
    print fs


    # train_df = full_df[ full_df['pd'] == 0 ]

    y = train_df['Y']

    y_real = [ int(i)  for i in y.values ]
    y_real_pred = [ int(i) for i in full_df[ full_df['pd'] ==1 ]['Y'].values ]

    #为了最后选出 最可能买哪一个商品
    user_sku_pred_df = full_df[ full_df['pd'] ==1 ][ [ 'user_id','sku_id' ] ]
    print user_sku_pred_df.head()

    X = full_df.iloc[:, fs]
    print 'dummies' * 10

    col_dfs = []
    for col in X.columns:
        # if col not in ['user_id','sku_id','model_id']:
        # if col   in ['user_id','sku_id']:
        # if col   in ['sku_id']:
        if col in ['user_id','sku_id']:continue
        
        if col   in ['sku_id']:
            print col , ' begins'
            dum = pd.get_dummies(X[col],prefix=col,sparse=True)
            print 'len = '  , len(dum.columns)

            col_dfs.append(dum)
            # dum.to_dense().to_csv('{col}.csv'.format(col=col))
            # dum.head().to_csv( '{col}.csv'.format(col = col ),chunksize=1000)
            print col , ' done'
        else:
            """
            use the real value
            """
            print col , ' real value'
            col_dfs.append(X[[col]])
            print col , ' done'


    # dump_svmlight_file(pd.concat(col_dfs  ,  axis=1 ).as_matrix() , full_df['Y'].astype(float), 'full.withuid.svmlight')
    # raise "column dummy ok"
    # X =  pd.get_dummies(X,sparse=True)
    X = pd.concat(col_dfs, axis =1)
    X = X.fillna(0)

    # print X.head()

    train_X = X [ X['pd'] == 0 ]
    pred_X = X[ X['pd'] == 1 ]


    print 'dummies ok'


    lr  = LogisticRegression(C=1 , penalty='l1', tol=0.0001,verbose=True)
    lr.fit(train_X, y)


    y_hat = lr.predict_proba(train_X)
    y_pos_hat = [ i[1] for i in y_hat]

    print 'auc = {0}'.format(roc_auc_score(y_real ,y_pos_hat))


    #具体是probability还是别的，需要在考虑
    y_hat = lr.predict_proba(pred_X)
    y_pos_hat = [ i[1] for i in y_hat]

    user_sku_pred_df['score'] = y_pos_hat

    # user_sku_pred_df = user_sku_pred_df[ user_sku_pred_df['score'] > 0.5 ]



    y_pred = np.argmax(y_hat, axis=1)  #axis is necessary .

    print 'auc = {0}'.format(roc_auc_score(y_real_pred ,y_pos_hat))
    print 'f1score = {0}'.format(f1_score(y_real_pred ,y_pred))


    print "find best sku_id "  + '-'* 10
    #best pos sku_ids  of user_id
    us = []
    sku_s = []
    for user_id , g  in user_sku_pred_df.groupby('user_id') :
        user_id = "%.0f" % float(user_id)

        mscore = g['score'].max()

        sku_id =  g[ g['score'] == mscore ]['sku_id'].values[0]
        if mscore >0.5:
            us.append(user_id )


            sku_s.append(sku_id)
            print user_id, sku_id,mscore

    result = pd.DataFrame(data={'sku_id':sku_s ,  'user_id':us  })
    # result.to_csv(bingo_csv,columns=['user_id','sku_id'] , index=False)
    result.to_csv('bingo.without_userid.csv',columns=['user_id','sku_id'] , index=False)


    # print 'train_ok'
    # subprocess.check_call(' echo train_ok | mail -s coach baipeng1@xiaomi.com ', shell=True)


"""
1.浏览（指浏览商品详情页）；
 2.加入购物车；3.购物车删除；4.下单；5.关注；6.点击
"""

def transfer_action():
    print 'read action '
    action_df = pd.read_csv('JData_Action_201604.csv')

    print 'user_sku_type_df2'
    action_df['time'] = action_df['time'].str[0:10]


    user_sku_type_df = action_df[['sku_id','user_id','time' , 'type']].groupby(['sku_id','user_id','time' , 'type']).size().reset_index()
    user_sku_type_df2 = pd.pivot_table(user_sku_type_df, values=0, index=['sku_id','user_id','time' ] , columns='type')
    print user_sku_type_df2.columns
    user_sku_type_df2['buy_view'] = user_sku_type_df2[4]/(user_sku_type_df2[1] +1)
    user_sku_type_df2['buy_click']= user_sku_type_df2[4]/(user_sku_type_df2[6] +1)
    user_sku_type_df2['buy_focus'] = user_sku_type_df2[4]/(user_sku_type_df2[5] +1)
    user_sku_type_df2['buy_cart']= user_sku_type_df2[4]/(user_sku_type_df2[2] +1)


    user_sku_type_df2.to_csv('user_sku_type.csv')


    print 'user_type_df2'

    user_type_df = action_df[['user_id','time','type']].groupby(['user_id','time','type']).size().reset_index()
    user_type_df2 = pd.pivot_table(user_type_df , values=0 , index=['user_id','time'] , columns= 'type')

    user_type_df2['buy_view'] = user_type_df2[4]/(user_type_df2[1] +1)
    user_type_df2['buy_click']= user_type_df2[4]/(user_type_df2[6] +1)
    user_type_df2['buy_focus'] = user_type_df2[4]/(user_type_df2[5] +1)
    user_type_df2['buy_cart']= user_type_df2[4]/(user_type_df2[2] +1)
    user_type_df2.to_csv('user_type.csv')


    print 'sk_type_df2'
    sku_type_df = action_df[['sku_id','time','type']].groupby(['sku_id','time','type']).size().reset_index()
    sku_type_df2 = pd.pivot_table(sku_type_df, values=0 , index=['sku_id','time'],columns= 'type')
    sku_type_df2['buy_view'] = sku_type_df2[4]/(sku_type_df2[1] +1)
    sku_type_df2['buy_click']= sku_type_df2[4]/(sku_type_df2[6] +1)
    sku_type_df2['buy_focus'] = sku_type_df2[4]/(sku_type_df2[5] +1)
    sku_type_df2['buy_cart']= sku_type_df2[4]/(sku_type_df2[2] +1)

    sku_type_df2.to_csv('sku_type.csv')


def action_merge():
    ust = pd.read_csv('user_sku_type.csv')
    ut = pd.read_csv('user_type.csv')
    st = pd.read_csv('sku_type.csv')

    user_merge = pd.merge( ust,ut ,on=['user_id','time'] )

    user_sku_merge = pd.merge(user_merge,st, on =['sku_id','time'])

    for name ,g in user_sku_merge.groupby( user_sku_merge['time']) :
        g.to_csv('./partial2/' + name + '.csv')


def main():
    print '-'*10 + 'main ' + '-'*10
    user_df = pd.read_csv(user_csv)
    prod_df = pd.read_csv(prod_csv)
    comment_df = pd.read_csv(comment_csv)

    print comment_df.groupby('dt').count()

    # action_df = pd.read_csv(headact_csv)
    action_df = pd.read_csv(action_csv)

    label_aux_df = action_df[ action_df['type'] == 4 ][['time','user_id','sku_id']]
    label_aux_df.to_csv(label_aux_csv)

    action_df['time'] = action_df['time'].str[0:10]

    #action count
    action_df = action_df[['sku_id','user_id','time' , 'type']].groupby(['sku_id','user_id','time' , 'type']).size().reset_index()


    act_user_df = pd.merge(action_df,user_df,left_on='user_id',right_on='user_id')


    act_user_prod_df = pd.merge(act_user_df,prod_df,left_on='sku_id',right_on='sku_id')




    act_user_prod_df.to_csv('out.size.csv')

    # subprocess.check_call(' echo main_ok | mail -s coach baipeng1@xiaomi.com ',shell=True)
    print act_user_prod_df.head()



if __name__ == '__main__':
    train()

    if sys.argv[1] == 'main':
        main()

    elif sys.argv[1] == 'each_sample':
        out = sys.argv[2]
        inf = sys.argv[3]
        print '*'*10
        build_sample_df(out , inf)
        print '^'*10
    elif sys.argv[1] == 'train':
        train()
    elif sys.argv[1] == 'flow':
        raise  'Not Support'
        main()
        # build_sample()
        train()
    elif sys.argv[1] == 'split':
        split_action()
    elif sys.argv[1] == 'transfer_label':
        transfer_label()
    elif sys.argv[1] == 'transfer_comment':
        transfer_comment()
    elif sys.argv[1] == 'transfer_action':
        transfer_action()
    elif sys.argv[1] == 'merge_action':
        action_merge()
    elif sys.argv[1] == 'concat':
        concat_sample()
    else:
        pass
