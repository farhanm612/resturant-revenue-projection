import pypyodbc
import pandas as pd
import datetime
#import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
#from sklearn import metrics
import pickle

def getStores(con):
    stores = []
    sql = " select distinct storeid from ProjectionDataPoints where day_part = 'catering'"
    cursor = con.cursor()
    cursor.execute(sql)
    for row in cursor.fetchall():
        stores.append(row[0])
    return stores

def model(storeid):
    date = datetime.datetime.now().date()
    sql = """ select p.storeid,p.projection_date,p.day_part,p.dp1,p.dp2,
                p.average,p.temp,
                (select SUM(netsales) from DpvHstCheckSummary where FKStoreId=p.StoreId and DateOfBusiness=p.projection_date and FKRevenueID not in (1,2))
                as actual from ProjectionDatapoints p where p.projection_date < '2018-08-09'  and p.storeid = %i and day_part = 'catering'""" %storeid
    df = pd.read_sql(sql, connection)

    df.dropna(axis=0, how='any', inplace=True)
    X_train, X_test, y_train, y_test = train_test_split(df[["dp1", "dp2","average","temp"]], df.actual, test_size=0)
    linreg = LinearRegression(n_jobs=4)
    linreg.fit(X_train, y_train)
    pickled_model = pickle.dumps(linreg)
    sql_insert = "insert into ProjectionModels values (?,?,?,?)"
    cur = connection.cursor()
    cur.execute(sql_insert,[storeid,date,pickled_model,"catering"])
    cur.commit()




if __name__ == '__main__':
    connection = pypyodbc.connect(
        'Driver={SQL Server};''Server='+config.DATABASE_CONFIG['host']+';''Database='+config.DATABASE_CONFIG['dbname']+';''uid='+config.DATABASE_CONFIG['user']+';pwd='+config.DATABASE_CONFIG['password'])
    #for store in [148,149,150,151,152,153,154]:
    #for store in [115,138,132,109,118,126,129,106,135,112,121,127,107,130,124,10,133,104,119,125,105,136,116,139,122,128,102,117,140,123,134,111,103,120,131,137,100]:
    for store in getStores(connection):
        model(store)
        print store
    connection.close()
