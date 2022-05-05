import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('NFL MVP Prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

# open data/player_stats.csv.gz
player_stats = spark.read.csv('data/player_stats.csv.gz', header=True, inferSchema=True).cache()

# drop _c0 column
player_stats = player_stats.drop('_c0')

# remove rows where 'Rk' or 'Age' are null
player_stats = player_stats.na.drop(subset=["Rk", "Age"])

# open data/mvp_voting.csv
mvp_voting = spark.read.csv('data/mvp_voting.csv', header=True, inferSchema=True).cache()

# get set of all unique player names
mvp_names = mvp_voting.select('Player').distinct().collect()

# convert mvp_names to python list
mvp_names = [x[0] for x in mvp_names]

# remove all rows from player_stats where player name is not in mvp_names
player_stats = player_stats.filter(player_stats.Name.isin(mvp_names))

# save player_stats to CSV
player_stats.coalesce(1).write.csv('data/player_stats_cleaned.csv', header=True, mode='overwrite')