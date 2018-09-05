import pyodbc
import requests
import config

def getzipcode (storeid,con):
    cursor = con.cursor()
    sql = "select zipcode from gblstore where storeid = (?)"
    cursor.execute(sql, storeid)
    return cursor.fetchone()[0]


def getdateRange (storeid,con):
    cursor = con.cursor()
    sql = "select min(projection_date),max(projection_date) from ProjectionDatapoints where temp is null and storeid = (?)"
    cursor.execute(sql, storeid)
    dt = cursor.fetchall()
    return dt[0][0],dt[0][1]

def getStores(con):
    stores = []
    sql = " select distinct storeid from ProjectionDatapoints where day_part = 'catering' "
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
        print store
        zipcode = getzipcode(store,connection)
        start,end = getdateRange(store,connection)
        #start = '2018-04-01'
        end = '2018-08-9'
        print start,end,zipcode
        url = """http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=%s&q=%s&format=json&date=%s&enddate=%s""" %(config.WEATHER_API['key'],zipcode,start,end)
        resp = requests.get(url)
        if resp.json()['data'].keys()[0] == 'error':
            continue
        for day in resp.json()['data']['weather']:
            date = day['date']
            temp = (int(day['maxtempF'])+int(day['mintempF']))/2
            print date
            sql = "update ProjectionDatapoints set temp = ? where storeid = %s and projection_date = '%s' " %(store, date)
            cursor.execute(sql, temp)
            connection.commit()
