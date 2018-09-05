from datetime import date,timedelta
import datetime
import pyodbc
import pandas as pd
from sqlalchemy import create_engine

def getDateString(date):
    return date.strftime("%Y-%m-%d")

def connect():
    return pyodbc.connect(
        'Driver={SQL Server};''Server='+config.DATABASE_CONFIG['host']+';''Database='+config.DATABASE_CONFIG['dbname']+';''uid='+config.DATABASE_CONFIG['user']+';pwd='+config.DATABASE_CONFIG['password'])


def getStores(con,end,start):
    stores = []
    sql = " select storeid from gblstore where opendate between '%s' and '%s' and storeid not in (0,101,113,114,901) " %(getDateString(start),getDateString(end))
    cursor = con.cursor()
    cursor.execute(sql)
    for row in cursor.fetchall():
        stores.append(row[0])
    return stores

def dateiter(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)


def dateRange(date,weeks,direction='backward'):
    dates = []
    for i in range(1,weeks):
        if direction == 'backward':
            diff = i*-7
        else:
            diff = i*7
        dates.append(getDateString(date + timedelta(days=diff)))
    return dates


def getDataPoint(daterange,con,storeid,colName):
    placeholders = ','.join('?' for i in range(len(daterange)))  # '?,?'
    sql = """select dateofbusiness, round(SUM(netsales),2) as %s,
                 'catering' as day_part from DpvHstCheckSummary
                 where FKStoreId = %i and DateOfBusiness in (%s) and FKRevenueID not in (1,2)
                 group by dateofbusiness """ %(colName,storeid,placeholders)
    df = pd.read_sql(sql,con,params=daterange)
    if df.shape[0] == 0:
        return pd.DataFrame(data={'day_part': ['catering'], colName: [0]})
    else:
        df = df.loc[df[colName] != 0]
        #print df.groupby(["day_part"],as_index=False).mean()
        return df.groupby(["day_part"],as_index=False).mean()

def getWeeklyAvg(date,con,storeid,colName,start=-14,end=-7):
    start_date = getDateString(date + timedelta(days=start))
    end_date = getDateString(date + timedelta(days=end))
    sql = """ select sum(netsales)/7 as %s,
                'catering' as day_part
                from DpvHstCheckSummary where FKStoreId = %i
                and DateOfBusiness between '%s' and '%s' and FKRevenueID not in (1,2)
                """ %(colName,storeid,start_date,end_date)
    df = pd.read_sql(sql, con)
    return df

def getSamedayLastWeek(date,con,storeid,colName):
    sql = """ select sum(netsales) as %s,
                'catering' as day_part
                from DpvHstCheckSummary where FKStoreId = %i
                and DateOfBusiness = dateadd(day,-14,'%s') and FKRevenueID not in (1,2)
                """ %(colName,storeid,date)
    df = pd.read_sql(sql, con)
    return df

def features(date, storeid, con):
    dp1_range = dateRange(date, 6)
    dp1 = getDataPoint(dp1_range, con, storeid, 'dp1')
    dp2 = getSamedayLastWeek(date,con,storeid,'dp2')
    average = getWeeklyAvg(date, con, storeid, "average")
    result = dp1.join(dp2.set_index('day_part'), how='inner', on='day_part')
    result = result.join(average.set_index('day_part'), how='inner', on='day_part')
    result['projection_date'] = getDateString(date)
    result['storeid'] = storeid
    result = result[
        ["storeid", "projection_date", "day_part", "dp1", "dp2", "average"]]
    result = result.round(2)
    return result


if __name__ == '__main__':
    con = connect()
    start_date = date(2018, 8,24)
    end_date = date(2018, 8, 31)
    stores = getStores(con, end_date + timedelta(days=-42),end_date + timedelta(days=-210))
    print stores
    insert = pd.DataFrame()
    for store in [148,149,150,151,152,153,154,155]:
    #for store in [155]:
        for single_date in dateiter(start_date, end_date):
            if single_date.weekday() == 6:
                continue
            else:
                print single_date
                print store
                insert = insert.append(features(single_date,store,con))
    print insert
    con.close()
    dbengine = create_engine("mssql+pyodbc://"+config.DATABASE_CONFIG['user']+":"+config.DATABASE_CONFIG['password']+"@"+config.DATABASE_CONFIG['host']+"/"+config.DATABASE_CONFIG['dbname'],creator=connect, echo=True)
    insert.to_sql("ProjectionDatapoints", dbengine, if_exists='append', chunksize=500, index=False)
