# CSED 516 Mini-Homework 1 Mengying Bi

## Objectives: GraphX on Spark
Tools: 
- Spark and GraphX
- Amazon Web Services
## What to Turn In
Turn in command and results of a query using GraphX on Spark.

## Assignment Details

- Deploy a EMR cluster with Spark and ingest the flights dataset as specified in the GraphX section (PDF).

```python
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("mini_hw1") \
    .getOrCreate()

# Obtain departure Delays data
delay = spark.read.csv("s3n://csed516/Flights/flights.csv", header="true", inferSchema="true")
delay.registerTempTable("departureDelays")
delay.cache()
print(delay.columns)

# Obtain airports dataset
airports = spark.read.csv("s3n://csed516/Flights/airports.csv", header="true", inferSchema="true")
airports.registerTempTable("airports")
print(airports.columns)

# create trip IATA codes table
tripIATA = sqlContext.sql("""
  SELECT DISTINCT IATA FROM(
    SELECT DISTINCT ORIGIN AS iata FROM departureDelays 
    UNION ALL SELECT DISTINCT DEST AS iata FROM departureDelays) x""")
tripIATA.registerTempTable("tripIATA")

# Merge airport data with tripIATA data
airports = sqlContext.sql("""
   SELECT IATA_CODE AS IATA, City, State, Country 
   FROM airports
   JOIN tripIATA trip ON trip.IATA = airports.IATA_CODE""")
airports.registerTempTable("airports")
airports.cache()

# Build `departureDelays_geo` DataFrame
# Obtain key attributes such as Date of flight, delays, distance,
# and airport information (Origin, Destination)
departureDelays_geo = sqlContext.sql("""
    SELECT CAST(f.FL_DATE as int) as tripid,
           CAST(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT('2017-', CONCAT(CONCAT(SUBSTR(CAST(f.FL_DATE as string), 1, 2), '-')), SUBSTR(CAST(f.FL_DATE AS string), 3, 2)), ' '), SUBSTR(CAST(f.FL_DATE AS string), 5, 2)), ':'), SUBSTR(CAST(f.FL_DATE AS string), 7, 2)), ':00') as timestamp) as localdate,
           CAST(f.DEP_DELAY as int) as delay, 
           f.ORIGIN AS src, f.DEST AS dst, o.city AS city_src,
           d.city AS city_dst, 
           o.state AS state_src,
           d.state AS state_dst
    FROM departureDelays f 
    JOIN airports o 
        ON o.IATA = f.ORIGIN 
    JOIN airports d 
        ON d.IATA = f.DEST""")
departureDelays_geo.registerTempTable("departureDelays_geo")

# Cache and Count
departureDelays_geo.cache()
departureDelays_geo.count()
```

- List the number of vertices and edges in the graph. Recall the graph has airports as the vertices and flights as edges. We ran this query in the section, so this task is just to check that you successfully created the graph (0 points)
```python
from pyspark.sql.functions import *
from graphframes import *

tripVertices = airports.withColumnRenamed("IATA", "id").distinct()
tripEdges = departureDelays_geo.select("tripid", "delay", "src", "dst", "city_dst", "state_dst")

# Cache the graph
tripEdges.cache()
tripVertices.cache()

# Examine the vertices and edges
tripVertices.show()
tripEdges.show()

# Build the graph
tripGraph = GraphFrame(tripVertices, tripEdges)
print tripGraph

# Project columns to build the smaller datastructure
tripEdgesPrime = departureDelays_geo.select("tripid", "delay", "src", "dst")
tripGraphPrime = GraphFrame(tripVertices, tripEdgesPrime)
#number of vertices and edges?
print "Airports: %d" % tripGraph.vertices.count()
print "Trips: %d" % tripGraph.edges.count()
```
```
Airports: 299
Trips: 1526121
```

- Which airport has the most number of flights (total flights incoming and outgoing)? (10 points)

```python
tripGraph.degrees.sort(desc("degree")).head(1)
```
```
[Row(id=u'ATL', degree=193125)]
The airport with the most number of flights is ATL, with 193125 in total.
```

- List the top-30 airports in Page Rank order (10 points)

```python
# Run PageRank algorithm, and show results.
results = tripGraph.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").sort("pagerank",ascending=False).take(30)
```
```
[Row(id=u'ATL', pagerank=18.94035344317982),
 Row(id=u'ORD', pagerank=15.075045933905992),
 Row(id=u'DEN', pagerank=12.077673683793895),
 Row(id=u'LAX', pagerank=11.087205765693602),
 Row(id=u'DFW', pagerank=9.371725090750106),
 Row(id=u'SFO', pagerank=9.112308905290758),
 Row(id=u'SEA', pagerank=7.858366914119087),
 Row(id=u'PHX', pagerank=7.660053586576233),
 Row(id=u'LAS', pagerank=7.640562780154049),
 Row(id=u'MSP', pagerank=7.46256513039359),
 Row(id=u'MCO', pagerank=6.872546778864025),
 Row(id=u'IAH', pagerank=6.788605577015002),
 Row(id=u'DTW', pagerank=6.463561452881664),
 Row(id=u'BOS', pagerank=6.431703967882096),
 Row(id=u'SLC', pagerank=5.921566639479554),
 Row(id=u'EWR', pagerank=5.79108633089163),
 Row(id=u'CLT', pagerank=5.616949417169181),
 Row(id=u'BWI', pagerank=5.3591629165023615),
 Row(id=u'JFK', pagerank=4.9186114622316675),
 Row(id=u'MDW', pagerank=4.6730483476892335),
 Row(id=u'LGA', pagerank=4.555018999555448),
 Row(id=u'SAN', pagerank=4.548271420655757),
 Row(id=u'FLL', pagerank=4.476021461068475),
 Row(id=u'PHL', pagerank=3.7721522914451384),
 Row(id=u'DCA', pagerank=3.6714028512566834),
 Row(id=u'PDX', pagerank=3.5037174072951873),
 Row(id=u'TPA', pagerank=3.4304954857929935),
 Row(id=u'DAL', pagerank=3.394355053786046),
 Row(id=u'MIA', pagerank=3.386630165983122),
 Row(id=u'STL', pagerank=2.860373039125456)]
```


