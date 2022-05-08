from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.getOrCreate()

# In this example we will use a dataset about the covid cases in US
# which columns are:
# date | county | state | fip | cases | deaths
# which types are:
# date | string | string | integer | integer | integer

# Our objetive here is to make a dataframe with the seven last days of data per state that we have as column
# in the new data frame, so we want a data frame with the columns:
# state | day_1 | day_2 | ... | day_7 | day_1_cases | ... | day_7_cases | day_1_deaths | ... | day_7_deaths
# using one udf that will receve all the data that per state and will return the new columns as a structure.


# Fist of all last see how to create a schema using the StrucType.
# Our dataset can be seen as a Structure that have six fields, being, 1 timestamp,
# 2 string and 3 integers, and which can consider that four fists aren't nullable but the 
# twos are. So our schema is:

schema = T.StructType(
    [
        T.StructField('date', T.DateType(), False),
        T.StructField('county', T.StringType(), False),
        T.StructField('state', T.StringType(), False),
        T.StructField('fip', T.IntegerType(), False),
        T.StructField('cases', T.IntegerType(), True),
        T.StructField('deaths', T.IntegerType(), True)
    ]
)

# if a field is a array of integer we identify its type as T.ArrayType(T.IntegerType())
# the field could even be another structure, and so we shuld identify its type as 
# T.StructType([T.StructField('field1', T.Type1Type, True),...]).

df = spark.read.csv('dataset.csv', header=True, schema=schema) 

# As we want the data per state we must sum the cases and death per state and date, so 

df = df.groupby(['state', 'date']).agg(F.sum('cases').alias('cases'), F.sum('deaths').alias('deaths'))

# udfs just receve one parameter as a argument so we have to pack the columns date, cases and deaths in just one column,
# we will do it creating a structure column called data.

df = df.withColumn('data', F.struct('date', 'cases', 'deaths'))

# and we want to pass all datas per state to the udf, so we will put all the data of the same state in a list

df = df.groupby('state').agg(F.collect_list('data').alias('data'))


#now we can create our function

def create_columnsS(par):
    #par will be a list of objects called rows that behaves like dictionaries
    
    dates = []
    
    for i in range(len(par)):
        dates.append((par[i]['date'], i))
        
        
    dates.sort() # this will give us the dates in the order and the index to acess the date in 
    # that day contained in row
    
    dates = [dates[0]]*(7-len(dates)) + dates #just in case we donte have seven days in our list
    
    
    
    day_1_date = dates[-7][0]
    day_2_date = dates[-6][0]
    day_3_date = dates[-5][0]
    day_4_date = dates[-4][0]
    day_5_date = dates[-3][0]
    day_6_date = dates[-2][0]
    day_7_date = dates[-1][0]
    
    day_1_cases = par[dates[-7][1]]['cases']
    day_2_cases = par[dates[-6][1]]['cases']
    day_3_cases = par[dates[-5][1]]['cases']
    day_4_cases = par[dates[-4][1]]['cases']
    day_5_cases = par[dates[-3][1]]['cases']
    day_6_cases = par[dates[-2][1]]['cases']
    day_7_cases = par[dates[-1][1]]['cases']
    
    day_1_deaths = par[dates[-7][1]]['deaths']
    day_2_deaths = par[dates[-6][1]]['deaths']
    day_3_deaths = par[dates[-5][1]]['deaths']
    day_4_deaths = par[dates[-4][1]]['deaths']
    day_5_deaths = par[dates[-3][1]]['deaths']
    day_6_deaths = par[dates[-2][1]]['deaths']
    day_7_deaths = par[dates[-1][1]]['deaths']
    
    # we received the data as a list of rows, but we can return it as a dictionary, because we will especify
    # the type(schema) of the returning object and so pyspark will convert it to row
    
    returning_dict = {
    'day_1': {
    'date': day_1_date,
    'cases': day_1_cases,
    'deaths': day_1_deaths
    }, 
    'day_2': {
    'date': day_2_date,
    'cases': day_2_cases,
    'deaths': day_2_deaths
    }, 
    'day_3': {
    'date': day_3_date,
    'cases': day_3_cases,
    'deaths': day_3_deaths
    }, 
    'day_4': {
    'date': day_4_date,
    'cases': day_4_cases,
    'deaths': day_4_deaths
    }, 
    'day_5': {
    'date': day_5_date,
    'cases': day_5_cases,
    'deaths': day_5_deaths
    }, 
    'day_6': {
    'date': day_6_date,
    'cases': day_6_cases,
    'deaths': day_6_deaths
    }, 
    'day_7': {
    'date': day_7_date,
    'cases': day_7_cases,
    'deaths': day_7_deaths
    }
    }
    
    return returning_dict
    
# therefore the returning type is

returning_type = T.StructType(
    [
        T.StructField('day_1', T.StructType([
                                             T.StructField('date', T.DateType(), True),
                                             T.StructField('cases', T.IntegerType(), True),
                                             T.StructField('deaths', T.IntegerType(), True)
                                               
                                             ]), True),
        T.StructField('day_2', T.StructType([
                                             T.StructField('date', T.DateType(), True),
                                             T.StructField('cases', T.IntegerType(), True),
                                             T.StructField('deaths', T.IntegerType(), True)
                                               
                                             ]), True),
        T.StructField('day_3', T.StructType([
                                             T.StructField('date', T.DateType(), True),
                                             T.StructField('cases', T.IntegerType(), True),
                                             T.StructField('deaths', T.IntegerType(), True)
                                               
                                             ]), True),                                      
        T.StructField('day_4', T.StructType([
                                             T.StructField('date', T.DateType(), True),
                                             T.StructField('cases', T.IntegerType(), True),
                                             T.StructField('deaths', T.IntegerType(), True)
                                               
                                             ]), True),
        T.StructField('day_5', T.StructType([
                                             T.StructField('date', T.DateType(), True),
                                             T.StructField('cases', T.IntegerType(), True),
                                             T.StructField('deaths', T.IntegerType(), True)
                                               
                                             ]), True),
        T.StructField('day_6', T.StructType([
                                             T.StructField('date', T.DateType(), True),
                                             T.StructField('cases', T.IntegerType(), True),
                                             T.StructField('deaths', T.IntegerType(), True)
                                               
                                             ]), True),
                                             
        T.StructField('day_7', T.StructType([
                                             T.StructField('date', T.DateType(), True),
                                             T.StructField('cases', T.IntegerType(), True),
                                             T.StructField('deaths', T.IntegerType(), True)
                                               
                                             ]), True),

    ]

)

# ok, now we can create the udf

create_columns = F.udf(create_columnsS, returning_type)

df = df.withColumn('udf_result', create_columns('data'))

#now, printing the schema we see that we alredy have what we want, we just have to unpack the columns contained in the udf_result column

#df.printSchema()

# we could do it by writting df.select(df['state'], df['udf_result.day_1.date'].alias('date_1_date'), df['udf_result.day_1.cases'].alias(date_1_cases), ...)
# or we could use col that accepts the * that will return all the values contained in the structure, but it doesnt acept alias,
# so lets be lazy and use selectExpr that accepts the sintaxy selectExp('column as column_new_name', ...):
columns = []
for i in range(1,8):
    for field in ['date', 'cases', 'deaths']:
        columns.append(f'udf_result.day_{i}.{field} as day_{i}_{field}')

df = df.selectExpr('state', *columns)

df.printSchema()
df.show()
