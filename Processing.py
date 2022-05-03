import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('NFL MVP Prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

# The schema for the data we are loading
player_stats_schema = types.StructType([
    types.StructField('_c0', types.IntegerType()),
    types.StructField('Rk', types.IntegerType()),
    types.StructField('Year', types.IntegerType()),
    types.StructField('Date', types.StringType()),
    types.StructField('G#', types.IntegerType()),
    types.StructField('Week', types.IntegerType()),
    types.StructField('Age', types.IntegerType()),
    types.StructField('Tm', types.StringType()),
    types.StructField('At', types.StringType()),
    types.StructField('Opp', types.StringType()),
    types.StructField('Result', types.StringType()),
    types.StructField('GS', types.IntegerType()),
    types.StructField('Sk', types.IntegerType()),
    types.StructField('Solo', types.IntegerType()),
    types.StructField('Ast', types.IntegerType()),
    types.StructField('Comb', types.IntegerType()),
    types.StructField('TFL', types.IntegerType()),
    types.StructField('QBHits', types.IntegerType()),
    types.StructField('Num', types.IntegerType()),
    types.StructField('Pct', types.FloatType()),
    types.StructField('Num.1', types.IntegerType()),
    types.StructField('Pct.1', types.FloatType()),
    types.StructField('Num.2', types.IntegerType()),
    types.StructField('Pct.2', types.FloatType()),
    types.StructField('Status', types.StringType()),
    types.StructField('Name', types.StringType()),
    types.StructField('Birthday', types.StringType()),
    types.StructField('Tgt', types.IntegerType()),
    types.StructField('Rec', types.IntegerType()),
    types.StructField('Yds', types.IntegerType()),
    types.StructField('Y/R', types.FloatType()),
    types.StructField('TD', types.IntegerType()),
    types.StructField('Ctch%', types.FloatType()),
    types.StructField('Y/Tgt', types.FloatType()),
    types.StructField('Rt', types.FloatType()),
    types.StructField('Yds.1', types.IntegerType()),
    types.StructField('Y/Rt', types.FloatType()),
    types.StructField('TD.1', types.IntegerType()),
    types.StructField('Fmb', types.IntegerType()),
    types.StructField('FL', types.IntegerType()),
    types.StructField('FF', types.IntegerType()),
    types.StructField('FR', types.IntegerType()),
    types.StructField('Yds.2', types.IntegerType()),
    types.StructField('TD.2', types.IntegerType()),
    types.StructField('Att', types.IntegerType()),
    types.StructField('Y/A', types.FloatType()),
    types.StructField('Pts', types.IntegerType()),
    types.StructField('TD.3', types.IntegerType()),
    types.StructField('Int', types.IntegerType()),
    types.StructField('PD', types.IntegerType()),
    types.StructField('Yds.3', types.IntegerType()),
    types.StructField('TD.4', types.IntegerType()),
    types.StructField('Sfty', types.IntegerType()),
    types.StructField('Ret', types.IntegerType()),
    types.StructField('Y/R.1', types.FloatType()),
    types.StructField('Yds.4', types.IntegerType()),
    types.StructField('2PM', types.IntegerType()),
    types.StructField('Cmp', types.IntegerType()),
    types.StructField('Att.1', types.IntegerType()),    
    types.StructField('Cmp%', types.FloatType()),
    types.StructField('Rate', types.FloatType()),
    types.StructField('Y/A.1', types.FloatType()),
    types.StructField('AY/A', types.FloatType()),
    types.StructField('Yds.5', types.IntegerType()),
    types.StructField('TD.5', types.IntegerType()),
    types.StructField('Yds.6', types.IntegerType()),
    types.StructField('TD.6', types.IntegerType()),
    types.StructField('Sk.1', types.IntegerType()),
    types.StructField('Int.1', types.IntegerType()),
    types.StructField('Yds.7', types.IntegerType()),
    types.StructField('TD.7', types.IntegerType()),
    types.StructField('XPM', types.IntegerType()),
    types.StructField('XPA', types.IntegerType()),
    types.StructField('XP%', types.FloatType()),
    types.StructField('FGM', types.IntegerType()),
    types.StructField('FGA', types.IntegerType()),
    types.StructField('FG%', types.FloatType()),
    types.StructField('Pnt', types.IntegerType()),
    types.StructField('Y/P', types.FloatType()),
    types.StructField('Blck', types.IntegerType())
])

# open data/player_stats.csv.gz
player_stats = spark.read.csv('data/player_stats.csv.gz', header=True, schema=player_stats_schema).cache()

# drop _c0 column
player_stats = player_stats.drop('_c0')

# remove rows where 'Rk' or 'Age' are null
player_stats = player_stats.na.drop(subset=["Rk", "Age"])