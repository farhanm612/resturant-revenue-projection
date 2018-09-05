import pyodbc
import requests
import pandas as pd
import config

def getzipcode (storeid,con):
    cursor = con.cursor()
    sql = "select zipcode from gblstore where storeid = (?)"
    cursor.execute(sql, storeid)
    return cursor.fetchone()[0]

def getStores(con):
    stores = []
    sql = " select distinct storeid from ProjectionDatapoints where temp is null and projection_date >'2018-08-09'"
    cursor = con.cursor()
    cursor.execute(sql)
    for row in cursor.fetchall():
        stores.append(row[0])
    return stores


if __name__ == '__main__':
    connection = pyodbc.connect(
        'Driver={SQL Server};''Server='+config.DATABASE_CONFIG['host']+';''Database='+config.DATABASE_CONFIG['dbname']+';''uid='+config.DATABASE_CONFIG['user']+';pwd='+config.DATABASE_CONFIG['password'])
    cursor = connection.cursor()
    for store in getStores(connection):
        #print store
        zipcode = getzipcode(store,connection)

        start = '2018-08-24'
        end = '2018-08-31'
        dr = pd.date_range(start,end).date
        for date in dr:
            if date.weekday() == 6:
                continue
            print date
            url = """http://api.worldweatheronline.com/premium/v1/weather.ashx?key=%s&q=%s&format=json&date=%s""" %(config.WEATHER_API['key'],zipcode,date)
            resp = requests.get(url)
            for data in resp.json()['data']['weather']:
                temp = (int(data['maxtempF'])+int(data['mintempF']))/2
                sql = "update ProjectionDatapoints set temp = ? where storeid = %s and projection_date = '%s' " %(store,date)
                cursor.execute(sql, temp)
                connection.commit()
