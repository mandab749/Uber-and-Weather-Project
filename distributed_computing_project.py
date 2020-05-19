
# to run in command line

import pyspark

sc = pyspark.SparkContext("local[*]", "uber")


# load in data

uber_apr14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-apr14.csv")
uber_may14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-may14.csv")
uber_june14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-jun14.csv")
uber_july14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-jul14.csv")
uber_aug14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-aug14.csv")
uber_sep14 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-sep14.csv")
uber_janjun15 = sc.textFile("/usr/data/uber/uber-trip-data/uber-raw-data-janjune-15.csv")

# 2014 data is all in the same format, 2015 is a little different 


# union 2014 data 
full_data_uber_2014 = sc.union([uber_apr14, uber_may14, uber_june14, uber_july14, uber_aug14, uber_sep14])

# remove headers
full_data_uber_2014 = full_data_uber_2014.filter(lambda x: x!= '"Date/Time","Lat","Lon","Base"')

# to structure each line
def string_split(line):
    return line.split(",")

full_data_uber_2014 = full_data_uber_2014.map(string_split)


from datetime import date
import datetime
 

# remove " " from data
def clean_strings(line):
    return (line[0].replace('"', ''), line[1], line[2], line[3].replace('"', ''))

full_data_uber_2014 = full_data_uber_2014.map(clean_strings)

# make time a datetime object
def fix_time(line):
    return (datetime.datetime.strptime(line[0], '%m/%d/%Y %H:%M:%S'), line[1], line[2], line[3])

full_data_uber_2014 = full_data_uber_2014.map(fix_time)


# test case
full_data_uber_2014.take(1)[0][0].date() > date(2019,1,1)
# False

# say we want to filter for a certain month:
filter_for_april = full_data_uber_2014.filter(lambda x: x[0].month == 4)
filter_for_april.take(5)


# compute the average number trips per day in April:
def map_day(line):
    return (line[0].day, 1)

def reducer(left, right):
    return left + right

total_trips_per_day_april = full_data_uber_2014.filter(lambda x: x[0].month == 4).map(map_day).reduceByKey(reducer)


def mapper(line):
    return (line[0], line[0].month, line[1], line[2], line[3])

full_data_uber_2014_with_month = full_data_uber_2014.map(mapper)

def mapper(line):
    return line[0], float(line[1]), float(line[2]), line[3]

full_data_uber_2014 = full_data_uber_2014.map(mapper)


# to convert to a dataframe=====

full_data_uber_2014_df = full_data_uber_2014_with_month.toDF()

# set col names
full_data_uber_2014_df = full_data_uber_2014_df.selectExpr("_1 as datetime", "_2 as month", "_3 as lat", "_4 as lng", "_5 as base")


# Average number of trips per month 2014

full_data_uber_2014_df.createOrReplaceTempView("uber2014")

total_trips_per_month_2014 = sqlContext.sql("select month, count(month) number_trips from uber2014 group by month order by number_trips desc")
total_trips_per_month_2014.show(6)






# cleaning 2015 data
full_data_uber_2015 = uber_janjun15.filter(lambda x: x!= 'Dispatching_base_num,Pickup_date,Affiliated_base_num,locationID')






# to get first week of April:

filter_for_1st_week_april = full_data_uber_2014.filter(lambda x: x[0].month == 4 and x[0].day < 8)

def mapper(line):
    return (line[0], line[0].isoweekday(), line[1], line[2], line[3])

first_week_april_with_day = filter_for_1st_week_april.map(mapper)


def mapper(line):
    if line[1] == 1:
        day = "Monday"
    if line[1] == 2:
        day = "Tuesday"
    if line[1] == 3:
        day = "Wednesday"
    if line[1] == 4:
        day = "Thursday"
    if line[1] == 5:
        day = "Friday"
    if line[1] == 6:
        day = "Saturday"
    if line[1] == 7:
        day = "Sunday"
    return (line[0], day, line[2], line[3], line[4])

first_week_april_with_dayname = first_week_april_with_day.map(mapper)
first_week_april_with_dayname_df = first_week_april_with_dayname.toDF()
first_week_april_with_dayname_df.createOrReplaceTempView("april2014")

total_trips_perDay_FWO_april = sqlContext.sql("select _2, count(_2) total_trips from april2014 group by _2 order by total_trips desc")
total_trips_perDay_FWO_april.show(8)
