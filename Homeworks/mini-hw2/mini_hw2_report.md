# CSED 516 Mini-Homework 2 Mengying Bi

## Objectives:

Execute queries on [Vertica](https://www.vertica.com/)!

## Assignment tools

Docker on EC2 or your local machine with Vertica.  See the [section notes](https://courses.cs.washington.edu/courses/csed516/19au/sections/section7.pdf) for details on installation.

## What to Turn In

Turn in command and results of a query using Vertica on a toy dataset.  Submit everything as a single markdown, notebook, or PDF.

# Assignment Dataset

## Lobster Dataset

The data we will use for Vertica is a toy dataset for website http://lobste.rs, which is a Hacker News clone.  We'll create relations for this dataset, ingest data, and then run queries.

### Create Schema

Create relations for this dataset as follows:

```sql
CREATE SCHEMA lobsters;

CREATE TABLE lobsters.tags ( id integer NOT NULL, tag varchar(64));

CREATE TABLE lobsters.taggings (id integer NOT NULL, story_id integer NOT NULL, tag_id integer NOT NULL);

CREATE TABLE lobsters.hiddens (id integer NOT NULL, user_id integer NOT NULL, story_id integer NOT NULL);

CREATE TABLE lobsters.stories (
  id integer NOT NULL, 
  created_at TIMESTAMP, 
  description varchar(4095), 
  hotness float, 
  markeddown_description varchar(4095), 
  short_id varchar(255), 
  title varchar(1023), 
  upvotes integer, 
  downvotes integer, 
  url varchar(255), 
  user_id integer);
```

### Download data

Next, download the data for these relations:

* [hidden.csv](https://courses.cs.washington.edu/courses/csed516/19au/sections/hidden.csv)
* [tags.csv](https://courses.cs.washington.edu/courses/csed516/19au/sections/tags.csv)
* [taggingcsv](https://courses.cs.washington.edu/courses/csed516/19au/sections/taggings.csv)
* [stories.csv](https://courses.cs.washington.edu/courses/csed516/19au/sections/stories.csv)

### Ingesting data into the database

Finally, use the following Python script to load data into your Vertica database:

```py
import vertica_python as vp
import json

with open("args.json") as f:
    args = json.load(f)
conn = vp.connect(**args)
cur = conn.cursor()
cur.execute("select * from sample_table")
cur.fetchone()
cur.fetchall()

with open("taggings.csv", "rb") as f:
    cur.copy("COPY lobsters.taggings from stdin DELIMITER ','", f)
with open("hidden.csv", "rb") as f:
    cur.copy("COPY lobsters.hiddens from stdin DELIMITER ','", f)
with open("stories.csv", "rb") as f:
    cur.copy("COPY lobsters.stories from stdin DELIMITER ','", f)
```

### Sample Queries

To make sure things are working, try executing the following queries from your VSQL console:
```sql
select count(*) from lobsters.stories where user_id = 1;

select t.tag_id, count(*) from lobsters.stories as s inner join lobsters.taggings as t on s.hotness > 0 and t.story_id = s.id group by t.tag_id order by count(*) desc;

select s.id, s.title, s.upvotes, s.downvotes from lobsters.stories as s inner join lobsters.hiddens as h on created_at > '2017-01-23 00:00:00' and h.user_id <> 1 and h.story_id = s.id inner join lobsters.taggings as t on t.story_id = s.id;

select id, title from lobsters.stories 
where id in (( select story_id from lobsters.taggings where tag_id not in (2, 6, 7) ) union (select story_id from lobsters.hiddens where user_id not in (3, 4, 5) )) and hotness > 0 and created_at > '2017-01-23 00:00:00';
```

# Assignment Details

In this Assignment you will be required to
access a Vertica DBMS either via EC2 or locally.  In both cases we recommend installing Vertica in a Docker container as described in the section notes.

In this homework you will write a few queries on the Lobsters dataset.
For query A and B below, return the SQL and the rows returned.
You may restrict the columns you list for these two queries. 

For the last bullet, run the queries a couple times and
return mean runtime for each of the queries as well as the discussion of the resulting query times.  For this query there is no need to return number of rows or actual rows from the queries.

- Describe in one sentence the purpose of each of the queries in the previous section.  Which (if any) of these queries are especially well-suited for a column store?
```sql
select count(*) from lobsters.stories where user_id = 1;
```
- The query returns the total number of stories that read by the user with id = 1.
```
    11
(1 row)
```

```sql
select t.tag_id, count(*) from lobsters.stories as s inner join lobsters.taggings as t on s.hotness > 0 and t.story_id = s.id group by t.tag_id order by count(*) desc;
```
- The query returns the tag id, and the count of story id  in a decreasing order, where the corresponding stories have positive hotness.
```
 tag_id | count 
--------+-------
     20 |    42
     38 |    41
     47 |    40
...
(70 rows)
```

```sql
select s.id, s.title, s.upvotes, s.downvotes from lobsters.stories as s inner join lobsters.hiddens as h on created_at > '2017-01-23 00:00:00' and h.user_id <> 1 and h.story_id = s.id inner join lobsters.taggings as t on t.story_id = s.id;
```
- The query returns the story id, story title, number of upvotes and downvotes where the stories are tagged, the hidden user id are not 1, and that the stories are created after 2017-01-23 00:00:00.
```
  id  |   title    | upvotes | downvotes 
------+------------+---------+-----------
    1 | fOQtwNHfer |       0 |         1
    5 | LDdvggOaVl |       8 |        10
    7 | PvMRjHcLgr |       1 |         8
   14 | CsjbtnPBtK |       6 |         2
   40 | wBUnVIZKNF |       4 |         1
   44 | AgEjMdBNBJ |       3 |         5
...
```

```sql
select id, title from lobsters.stories 
where id in (( select story_id from lobsters.taggings where tag_id not in (2, 6, 7) ) union (select story_id from lobsters.hiddens where user_id not in (3, 4, 5) )) and hotness > 0 and created_at > '2017-01-23 00:00:00';
```
- The query returns the story id and title, with positive hotness, and created after 2017-01-23 00:00:00, and didn't get tagged by users with hidden user id 3,4,5 and the tag ids are not 2,6,7.
```
  id  |   title    
------+------------
    2 | bPHgwwweLa
    7 | PvMRjHcLgr
    9 | MzFAhmFNMU
   11 | efOFIzzkmm
   14 | CsjbtnPBtK
   15 | pfLzkHmIgU
   17 | GBfczJiRps
   20 | mjworAwwIZ
...
```
I think query 1 is suited for column store, since column store is optimal when looking to compute statistics on a specific column. So using column store is beneficial as it does not need to read all the data in each row to memory. In this case, we only read the user_id column to do the computation.

- Query A: List the top-10 stories ranked by 'hotness' (10 points)
```sql
select id, title, hotness
from lobsters.stories
order by hotness desc
limit 10;
```
```
  id  |   title    |    hotness    
------+------------+---------------
 1388 | GiyBTEsXBR | 99.9885090161
 2891 | qISUKItSwC | 99.9805785608
  781 | bZYQevPKFy | 99.9792039579
 2475 | zTJAhTqNgJ |  99.962903977
 3429 | xvriRtRrvs | 99.8850728073
 1286 | HaXZaHEWIT | 99.8716577729
 1883 | kjxhCdSYyR | 99.8222696379
 3801 | ZpkldiYJLx | 99.7675826054
 2216 | AsTZYwyOTf | 99.7612080507
 3291 | ZorLovUzWJ | 99.6313168876
```
- Query B: List top 10 stories with the highest number of up-votes and lowest number of down-votes. How does this list compare with the results from the previous query? (10 points)
```sql
select id, title, upvotes, downvotes
from lobsters.stories
order by upvotes desc, downvotes asc
limit 10;
```
```
 id  |   title    | upvotes | downvotes 
-----+------------+---------+-----------
 268 | CXxOEtHUyp |      10 |         0
 649 | FZzvBRTEQL |      10 |         0
 139 | WMDJnthHxR |      10 |         0
 130 | DdGuGfgMmR |      10 |         0
 435 | ZelrXpNakL |      10 |         0
  52 | rDefdxZwXE |      10 |         0
 101 | csShiBeiUh |      10 |         0
  21 | dVfQdzTVDk |      10 |         0
 100 | xlqwCAQYTd |      10 |         0
 907 | JnjSXezSjL |      10 |         0
(10 rows)
```
The result shows that the higher hotness score does not mean people like it more(or that it would get better votes). Since the stories that have the best vote is not the same the stories that are the most popular.

- For the `stories` relation run the following queries, note the time to run these queries and discuss your intuition and observations about their runtimes (10 points):
  - `select * from stories;`

    Time: First fetch (1000 rows): 31.310 ms. All rows formatted: 370.534 ms

  - `select id from lobsters.stories;`

    Time: First fetch (1000 rows): 7.969 ms. All rows formatted: 12.336 ms

  - `select id, title from lobsters.stories;`

    Time: First fetch (1000 rows): 8.894 ms. All rows formatted: 14.997 ms

    I used warm cache runtime and run for 5 times for each quary and calculated teh average runtime. The result is that the more number of columns return, the longer the runtime.

