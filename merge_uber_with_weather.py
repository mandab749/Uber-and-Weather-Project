# read in data
weather = sc.textFile("weather.csv")

# remove header
weather = weather.filter(lambda x: x!= 'date_time,humidity,wind,temp,description')

# split up data and map
def clean_weather(line):
    return line.split(",")

weather_clean = weather.map(clean_weather)


from datetime import date
import datetime


# setup datetime object and map
def fix_time(line):
    return (datetime.datetime.strptime(line[0], '%m/%d/%y %H:%M'), line[1], line[2], line[3], line[4])
    #return (datetime.datetime.strptime(line[0], '%Y-%m-%d %H:%M:%S'), line[1], line[2], line[3], line[4])
    
weather_clean_time = weather_clean.map(fix_time)



# get month, day and hour and map
def getTime(line):
    return line[0], line[0].month, line[0].day, line[0].hour, line[1], line[2], line[3], line[4]

weather_clean_time2 = weather_clean_time.map(getTime)

# do same for uber data
def getTimeUber(line):
    return line[0], line[0].month, line[0].day, line[0].hour, line[1], line[2], line[3]

full_data_uber_2014_time = full_data_uber_2014.map(getTimeUber)

# convert to dataframe
full_data_uber_2014_time_df = full_data_uber_2014_time.toDF()
weather_clean_time2_df = weather_clean_time2.toDF()

# name columns
full_data_uber_2014_time_df = full_data_uber_2014_time_df.selectExpr("_1 as datetime", "_2 as month", "_3 as day", "_4 as hour", "_5 as lat", "_6 as lng", "_7 as base")
weather_clean_time2_df = weather_clean_time2_df.selectExpr("_1 as datetime1", "_2 as month", "_3 as day", "_4 as hour", "_5 as humidity", "_6 as wind", "_7 as temp", "_8 as description") 

# join data
left_join = full_data_uber_2014_time_df.join(weather_clean_time2_df, (full_data_uber_2014_time_df.month == weather_clean_time2_df.month) & (full_data_uber_2014_time_df.day == weather_clean_time2_df.day) & (full_data_uber_2014_time_df.hour == weather_clean_time2_df.hour), how='left_outer') 

# remove columns we don't need
left_join = left_join.drop('datetime1')
left_join = left_join.drop('month')
left_join = left_join.drop('day')
left_join = left_join.drop('hour')
left_join.show(5)

# tried to save to csv 
#left_join.toPandas().to_csv('uber_data_with_weather.csv')

# convert back to RDD
rddd = left_join.rdd.map(tuple)

# to convert temp
def fixTemp(line):
    #return line[0], line[1], line[2], line[3], float(line[4]), float(line[5]), round((float(line[6]) - 273.15) * 9/5 + 32, 2), line[7]
    return line[0], line[1], line[2], line[3], float(line[4]), float(line[5]), float(line[6]), line[7]
    
rddd_temp_fixed = rddd.map(fixTemp)
rddd_temp_fixed.take(5)


# to make rain/no rain variable:

def rain(line):
    if line[7] == "scattered clouds" or line[7] == "sky is clear" or line[7] == "broken clouds" or line[7] == "haze" or line[7] == "few clouds" or line[7] == "overcast clouds" or line[7] == "mist" or line[7] == "fog" or line[7] == "dust" or line[7] == "smoke":
        dummy = "no rain"
    else:
        dummy = "rain"
    return line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], dummy


rddd_temp_fixed_rain = rddd_temp_fixed.map(rain)






################################ wrote this before fixing temp and adding rain ##################################

# get month, day and hour and map
def getTime(line):
    return line[0], line[0].month, line[0].day, line[0].hour, line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8]

rddd_temp_fixed_rain1 = rddd_temp_fixed_rain.map(getTime)

# move back to dataframe
rddd_temp_fixed_rain1_df = rddd_temp_fixed_rain1.toDF()

# set columns 
rddd_temp_fixed_rain1_df = rddd_temp_fixed_rain1_df.selectExpr("_1 as datetime1", "_2 as month", "_3 as day", "_4 as hour", "_5 as lat",  "_6 as lng", "_7 as base", "_8 as humidity", "_9 as wind", "_10 as temp", "_11 as description", "_12 as rain")
rddd_temp_fixed_rain1_df.show(5)

# to compute mean temperature by day by month for 2014
rddd_temp_fixed_rain1_df.createOrReplaceTempView("uber2014")
test = sqlContext.sql("select mean(temp), month, day from uber2014 group by month, day order by month, day")

