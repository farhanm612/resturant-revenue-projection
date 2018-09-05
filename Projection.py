import pyodbc
import pickle
from features import getDateString,dateiter
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
from sklearn import metrics

def getStores(con):
    stores = []
    sql = "  select distinct storeid from ProjectionDataPoints where projection_date > '2018-08-16' and dp3 is null  "
    cursor = con.cursor()
    cursor.execute(sql)
    for row in cursor.fetchall():
        stores.append(row[0])
    return stores

def connect():
    return pyodbc.connect(
        'Driver={SQL Server};''Server='+config.DATABASE_CONFIG['host']+';''Database='+config.DATABASE_CONFIG['dbname']+';''uid='+config.DATABASE_CONFIG['user']+';pwd='+config.DATABASE_CONFIG['password'])

def getfeatures(single_date, store, connection):
    sql = """select p.storeid,p.projection_date,p.day_part,p.dp1,p.dp2,p.dp3,p.average,p.temp from ProjectionDataPoints p
             where p.storeid = %s and p.projection_date = '%s' and p.day_part = 'catering'""" %(store,getDateString(single_date))
    df = pd.read_sql(sql,connection)
    return df

def project(df,store,connection):
    sql = "select top 1 Model from projectionmodels where status = 'catering' and store = %i order by model_date desc" %store
    cur = connection.cursor()
    cur.execute(sql)
    linreg = pickle.loads(cur.fetchone()[0])
    #print linreg.intercept_
    #print list(zip(["dp1", "dp2", "dp3","average"], list(linreg.coef_)))
    #x = df[["dp1","dp2","dp3","average","temp"]]
    x = df[["dp1","dp2","average","temp"]]
    y = linreg.predict(x)
    y = pd.DataFrame({'projection_date': df['projection_date'],
                      'storeid':df['storeid'],
                      'day_part':df['day_part'],
                      'revenue':y })
    dbengine = create_engine("mssql+pyodbc://"+config.DATABASE_CONFIG['user']+":"+config.DATABASE_CONFIG['password']+"@"+config.DATABASE_CONFIG['host']+"/"+config.DATABASE_CONFIG['dbname'],creator=connect, echo=True)
    y = y.round(2)
    y.to_sql("RegressionProjections", dbengine,if_exists='append',chunksize=500, index=False)
    print y


# TODO: Change Number of features depending on the age of store

if __name__ == '__main__':
    connection = connect()
    end_date = date(2018, 8, 31)
    #for store in getStores(connection):
    for store in getStores(connection):
        print store
        insert = pd.DataFrame()
        start_date = date(2018, 8, 24)
        for single_date in dateiter(start_date, end_date):
            if single_date.weekday() == 6:
                continue
            else:
                insert = insert.append(getfeatures(single_date, store, connection))
                #print insert
        project(insert,store,connection)
    connection.close()
