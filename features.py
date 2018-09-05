from datetime import date,timedelta
import datetime
import pyodbc
import pandas as pd
from sqlalchemy import create_engine


def getDateString(date):
    return date.strftime("%Y-%m-%d")


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
                 case when opentime<840 then 'opentotwo'
                 when OpenTime between 840 and 960 then 'twotofour'
                 when OpenTime > 960 then 'fourtoclose' end as day_part from DpvHstCheckSummary
                 where FKStoreId = %i and DateOfBusiness in (%s) and FKRevenueID in (1,2)
                 group by dateofbusiness,case when opentime<840 then 'opentotwo'
                 when OpenTime between 840 and 960 then 'twotofour'
                 when OpenTime > 960 then 'fourtoclose' end """ %(colName,storeid,placeholders)
    df = pd.read_sql(sql,con,params=daterange)
    df = df.loc[df[colName] != 0]
    return df.groupby(["day_part"],as_index=False).mean()


def getWeeklyAvg(date,con,storeid,colName,start=-14,end=-7):
    start_date = getDateString(date + timedelta(days=start))
    end_date = getDateString(date + timedelta(days=end))
    sql = """ select sum(netsales)/7 as %s,
                case when opentime<840 then 'opentotwo'
                when OpenTime between 840 and 960 then 'twotofour'
                when OpenTime > 960 then 'fourtoclose' end as day_part
                from DpvHstCheckSummary where FKStoreId = %i
                and DateOfBusiness between '%s' and '%s' and FKRevenueID in (1,2)
                group by case when opentime<840 then 'opentotwo'
                when OpenTime between 840 and 960 then 'twotofour'
                when OpenTime > 960 then 'fourtoclose' end""" %(colName,storeid,start_date,end_date)
    df = pd.read_sql(sql, con)
    return df


def features(date, storeid, con):
    dp1_range = dateRange(date, 6)
    dp2_range = dateRange(date + timedelta(days=-357), 6)
    dp3_range = dateRange(date + timedelta(days=-371), 6, direction='forward')
    dp1 = getDataPoint(dp1_range, con, storeid, 'dp1')
    dp2 = getDataPoint(dp2_range, con, storeid, 'dp2')
    dp3 = getDataPoint(dp3_range, con, storeid, 'dp3')

    dp1_4_range = dateRange(date, 5)
    dp2_4_range = dateRange(date + timedelta(days=-357), 5)
    dp3_4_range = dateRange(date + timedelta(days=-371), 5, direction='forward')
    dp1_4 = getDataPoint(dp1_4_range, con, storeid, 'dp1_4')
    dp2_4 = getDataPoint(dp2_4_range, con, storeid, 'dp2_4')
    dp3_4 = getDataPoint(dp3_4_range, con, storeid, 'dp3_4')

    dp1_3_range = dateRange(date, 4)
    dp2_3_range = dateRange(date + timedelta(days=-357), 4)
    dp3_3_range = dateRange(date + timedelta(days=-371), 4, direction='forward')
    dp1_3 = getDataPoint(dp1_3_range, con, storeid, 'dp1_3')
    dp2_3 = getDataPoint(dp2_3_range, con, storeid, 'dp2_3')
    dp3_3 = getDataPoint(dp3_3_range, con, storeid, 'dp3_3')

    dp1_6_range = dateRange(date, 7)
    dp2_6_range = dateRange(date + timedelta(days=-357), 7)
    dp3_6_range = dateRange(date + timedelta(days=-371), 7, direction='forward')
    dp1_6 = getDataPoint(dp1_6_range, con, storeid, 'dp1_6')
    dp2_6 = getDataPoint(dp2_6_range, con, storeid, 'dp2_6')
    dp3_6 = getDataPoint(dp3_6_range, con, storeid, 'dp3_6')

    dp1_7_range = dateRange(date, 8)
    dp2_7_range = dateRange(date + timedelta(days=-357), 8)
    dp3_7_range = dateRange(date + timedelta(days=-371), 8, direction='forward')
    dp1_7 = getDataPoint(dp1_7_range, con, storeid, 'dp1_7')
    dp2_7 = getDataPoint(dp2_7_range, con, storeid, 'dp2_7')
    dp3_7 = getDataPoint(dp3_7_range, con, storeid, 'dp3_7')

    average = getWeeklyAvg(date, con, storeid,"average")
    average_2 = getWeeklyAvg(date, con, storeid,"average_2",-21,-14)
    average_3 = getWeeklyAvg(date, con, storeid,"average_3",-28,-21)


    result = dp1.join(dp2.set_index('day_part'), how='inner', on='day_part')
    result = result.join(dp3.set_index('day_part'), how='inner', on='day_part')
    result = result.join(average.set_index('day_part'), how='inner', on='day_part')

    result = result.join(dp1_4.set_index('day_part'), how='inner', on='day_part')
    result = result.join(dp2_4.set_index('day_part'), how='inner', on='day_part')
    result = result.join(dp3_4.set_index('day_part'), how='inner', on='day_part')

    result = result.join(dp1_3.set_index('day_part'), how='inner', on='day_part')
    result = result.join(dp2_3.set_index('day_part'), how='inner', on='day_part')
    result = result.join(dp3_3.set_index('day_part'), how='inner', on='day_part')

    result = result.join(dp1_6.set_index('day_part'), how='inner', on='day_part')
    result = result.join(dp2_6.set_index('day_part'), how='inner', on='day_part')
    result = result.join(dp3_6.set_index('day_part'), how='inner', on='day_part')

    result = result.join(dp1_7.set_index('day_part'), how='inner', on='day_part')
    result = result.join(dp2_7.set_index('day_part'), how='inner', on='day_part')
    result = result.join(dp3_7.set_index('day_part'), how='inner', on='day_part')

    result = result.join(average_2.set_index('day_part'), how='inner', on='day_part')
    result = result.join(average_3.set_index('day_part'), how='inner', on='day_part')

    # result = pd.concat([dp1, dp2['dp2'],dp3['dp3'],average['average']], axis=1,join='inner')
    result['projection_date'] = getDateString(date)
    result['storeid'] = storeid
    result = result[["storeid", "projection_date", "day_part", "dp1", "dp2", "dp3", "average","dp1_4","dp2_4","dp3_4","dp1_3","dp2_3","dp3_3","dp1_6","dp2_6","dp3_6","dp1_7","dp2_7","dp3_7","average_2","average_3"]]
    result = result.round(2)
    return result


def connect():
    return pyodbc.connect(
        'Driver={SQL Server};''Server='+config.DATABASE_CONFIG['host']+';''Database='+config.DATABASE_CONFIG['dbname']+';''uid='+config.DATABASE_CONFIG['user']+';pwd='+config.DATABASE_CONFIG['password'])


def getStores(con,date):
    stores = []
    sql = " select storeid from gblstore where opendate < '%s' and storeid not in (0,101,113,114,901) " %getDateString(date)
    cursor = con.cursor()
    cursor.execute(sql)
    for row in cursor.fetchall():
        stores.append(row[0])
    return stores


def getRecentDate(con,store):
    sql = "select max(projection_date) from ProjectionDatapoints where storeid = %i" %store
    cursor = con.cursor()
    cursor.execute(sql)
    return cursor.fetchone()[0]


def dateiter(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)


if __name__ == '__main__':
    con = connect()
    #end_date = datetime.datetime.now().date()
    end_date = date(2018,8,31)
    stores = getStores(con,end_date + timedelta(days=-600))
    insert = pd.DataFrame()
    for store in [115,138,132,109,118,126,129,106,135,112,121,127,107,130,124,10,133,104,119,125,105,136,116,139,122,128,102,117,140,123,134,111,103,120,131,137,100]:
         #start_date = getRecentDate(con,store)
         #if start_date is None:
             #start_date = end_date + timedelta(days=-180)
         start_date = date(2018,8,24)
         for single_date in dateiter(start_date, end_date):
             if single_date.weekday() == 6:
                continue
             else:
                print single_date
                print store
                insert = insert.append(features(single_date,store,con))
    con.close()
    dbengine = create_engine("mssql+pyodbc://"+config.DATABASE_CONFIG['user']+":"+config.DATABASE_CONFIG['password']+"@"+config.DATABASE_CONFIG['host']+"/"+config.DATABASE_CONFIG['dbname'],creator=connect, echo=True)
    insert.to_sql("ProjectionDatapoints", dbengine,if_exists='append',chunksize=500, index=False)
