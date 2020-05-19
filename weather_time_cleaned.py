weather = sc.textFile("uber_weather.csv")

weather = weather.filter(lambda x: x!= 'datetime,lat,lng,base,humidity,wind,temp,description')


def clean_weather(line):
    return line.split(",")

weather_clean = weather.map(clean_weather)

from datetime import date
import datetime
from dateutil import parser

def fix_time(line):
    return(parser.parse(line[0]), line[1], line[2], line[3], line[4], line[5], line[6], line[7])

weather = weather_clean.map(fix_time)

def adddays(line):
    return line[0], line[0].day, line[0].month, line[1], line[2], line[3], line[4], line[5], line[6], line[7]

weather_days = weather.map(adddays)


def rain(line):
    if line[9] == "scattered clouds" or line[9] == "sky is clear" or line[9] == "broken clouds" or line[9] == "haze" or line[9] == "few clouds" or line[9] == "overcast clouds" or line[9] == "mist" or line[9] == "fog" or line[9] == "dust" or line[9] == "smoke":
        dummy = "no rain"
    else:
        dummy = "rain"
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], dummy


weather_days_rain = weather_days.map(rain)

def floater(line):
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], float(line[8]), line[9], line[10] 


weather1 = weather_days_rain.map(floater)

##############################################

import pandas as pd
from shapely.geometry import Point, Polygon
import geopandas as gpd
import fiona as fiona

#getting lat and long in proper format
def latlong(line):
     new = float(line[4]), float(line[3])
     return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], float(line[8]), line[9], line[10], new

latlong = weather1.map(latlong)

def point(line):
     poly = gpd.GeoDataFrame.from_file('geo_export_b697b323-ce5d-4268-8623-7712a657fd85.shp')
     point = Point(line[11])
     if poly.contains(point)[0] == True:
         boro = "Bronx"
     if poly.contains(point)[1] == True:
         boro = "Staten Island"
     if poly.contains(point)[2] == True:
         boro = "Brooklyn"
     if poly.contains(point)[3] == True:
         boro = "Queens"
     if poly.contains(point)[4] == True:
         boro = "Manhattan"
     else:
         boro = "Other"
     return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10],line[11], boro

latlongagain = latlong.map(point)

##############################################


weather_df = latlongagain.toDF()


weather_df = weather_df.selectExpr("_1 as datetime1", "_2 as day", "_3 as month", "_4 as lat", "_5 as lng",  "_6 as base", "_7 as humidity", "_8 as wind", "_9 as temp", "_10 as desc", "_11 as rain", "_12 as latlng", "_13 as borough")
weather_df.show(5)


weather_df.createOrReplaceTempView("uber2014")

bor_rain = sqlContext.sql("select borough, day, month, rain, count(rain) number_trips from uber2014 group by day, month, borough, rain order by day desc")
test = sqlContext.sql("select day, month, rain, count(rain) number_trips from uber2014 group by day, month, rain order by day desc")
test.createOrReplaceTempView("counttrips")
bor_rain.createOrReplaceTempView("counttrips_borough")

average_trips_per_day = sqlContext.sql("select mean(number_trips) mean_num_trips from counttrips")
average_trips_per_day_with_rain = sqlContext.sql("select rain, mean(number_trips) mean_num_trips from counttrips group by rain")
average_trips_per_month_with_rain = sqlContext.sql("select month, rain, mean(number_trips) mean_num_trips from counttrips group by month, rain order by month desc")

average_trips_per_month_with_rain.show(5)

