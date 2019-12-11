# Homework 2

## Due Tuesday, Oct 29th at 11:59pm

## Objectives: 

Learn how to use Snowflake, which is offered as a cloud service and is similar to Amazon Redshift.  In fact, Snowflake originally ran on Amazon Web Services and used EC2 clusters for compute and was backed by S3 for storage.  Today, Snowflake can run on top of other cloud platforms such as Microsoft Azure (as of 2018) and the Google Cloud Platform (as of 2019). 
In this assignment, we will set up a Snowflake database, ingest data, run some queries, and compare the experience with Redshift.

Reminder: Now that HW1 is done, don't forget to shut down your RedShift clusters!

## Assignment tools:
  - <a href="https://www.snowflake.com">Snowflake</a>!
  - <a href="https://bma81454.snowflakecomputing.com">Our course Snowflake cluster</a>

## What to turn in:
You will turn in:
  - SQL for the queries
  - Runtimes for each query
  - Number of rows returned
  - First two rows from the result set (or all rows if a query returns fewer than 2 rows) 
  - A brief discussion of the query runtimes that you observed in different settings

Submit everything as a single pdf or Markdown file.

## How to submit the assignment:

In your GitLab repository you should see a directory called `Homeworks/hw2`. Put your report in that directory. Remember to commit and push your report to GitLab (`git add && git commit && git push`)!

You should add your report early and keep updating it and pushing it as you do more work. We will collect the final version after the deadline passes. If you need extra time on an assignment, let us know. This is a graduate course, so we are reasonably flexible with deadlines but please do not overuse this flexibility. Use extra time only when you truly need it.

# Assignment Details

In this Assignment you will be required to work with Snowflake on AWS using a mini-cluster, ingest data, and run some queries on this data.

## 1. Setting up your Snowflake account (0 points)
  - Activate your Snowflake account by going to <a href="https://bma81454.snowflakecomputing.com">our course cluster</a>
  - In your browser, enter your username and temporary password (both your UW NetID) and set a new, secure password
  - Log into Snowflake using your new account
  - IMPORTANT: ensure that that correct role -- named "[Your UW NetID]Role" -- is shown in the upper-righthand corner of the Snowflake console.  Let us know on Piazza if you do not see it (or are in the "Public" role)
  - Optional: follow the "Getting Started" tutorial and familiarize yourself with the Snowflake interface

## 2. Create a new database (0 points)
  - Using the console:
    - Click the databases link
    - Click "Create..."
    - Give your database a _unique_ name
    - Select "Finish"
  - Using a worksheet
    - Click the "worksheet" link
    - Execute the SQL query `CREATE DATABASE [Your UW NetID]_myawesomedatabase;`

## 3. Ingest data (10 points)
  So that we can compare with Redshift, we'll again be using TPC-H datasets.  We'll import data both from S3 (as before) and from a separate database that we've set up in Snowflake.

  First we'll need to create tables.  Since both Snowflake and Redshift use SQL, we can use the <a href="../hw1/CreateTables.md">statements</a> from the previous homework to create our tables in Snowflake.  Use the worksheets tab to submit these queries, and remember that you can submit multiple statements at once.

### Ingesting data from another Snowflake database
  - We'll first import data that has already been ingested into Snowflake.  
  - To copy data from the existing TPC-H database, we can just `insert` the tuples:
  ```sql
     INSERT INTO customer
     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
     (SELECT * FROM snowflake_sample_data.tpch_sf10.customer);
  ```
  Note that we need to explicitly name the columns because the our schema contains an extra attribute, `skip`, that is not present in the Snowflake version of the data.  Lame.
  - Record the ingestion runtimes by table.  

### Ingesting data from S3
  - Next, we'll ingest data from S3.  
  - First, clear your tables: `truncate table customers;`.  Repeat for each TPC-H table.
  - In most databases, `truncate table R` and `delete * from R` have subtle differences that are often important in the real world, and Snowflake adds additional differentiation between these two options.  Look over the Snowflake documentation for <a href="https://docs.snowflake.net/manuals/sql-reference/sql/truncate-table.html">each</a> <a href="https://docs.snowflake.net/manuals/sql-reference/sql/delete.html">option</a>.  When would you prefer one over the other?
  - Next we need to define the formatting details of our external S3 data.  To do so, create a new file format by executing `create or replace file format [Your UW NetID]_csv type = csv field_delimiter = '|';`.  See <a href="https://docs.snowflake.net/manuals/sql-reference/sql/create-file-format.html">the documentation</a> for more details about file formats.
  - Now we need to create a new _stage_ in Snowflake.  A stage is an execution unit that compute will instantiate when ingesting internal or external data.  To create a stage, execute the command `create or replace stage [Your UW NetID]_customers url='s3://uwdb/tpch/uniform/10GB/customer.tbl' file_format = [Your UW NetID]_csv;`.  Create stages for each of the TPC-H tables.  See the <a href="https://docs.snowflake.net/manuals/sql-reference/sql/create-stage.html">documentation</a> for more details.
  - Ingest the data: `copy into customer from @[Your UW NetID]_customers;`
  - Record the ingestion runtimes by table.  Do the number of rows match what you found in Redshift?

## 3. Run Queries (25 points)

Using a worksheet, run each query listed below multiple times (yes, they're the same as in homework one!). Plot the average and either min/max or standard deviation.  Use the warm cache timing, which means you discard the first time the query is run. 

  - What is the total number of parts offered by each supplier? The query should return the name of the supplier and the total number of parts.
  - What is the cost of the most expensive part by any supplier? The query should return only the price of that most expensive part. No need to return the name.
  - What is the cost of the most expensive part for each supplier? The query should return the name of the supplier and the cost of the most expensive part but you do not need to return the name of that part. 
  - What is the total number of customers per nation? The query should return the name of the nation and the number of unique customers. 
  - What is number of parts shipped between 10 October, 1996 and 10 November, 1996 for each supplier? The query should return the name of the supplier and the number of parts.

## 4. Snowflake and Redshift Comparison (15 points)

  - Our Snowflake cluster runs on S3, which means both the `snowflake_sample_data.tpch_sf10` and `s3://uwdb/tpch/uniform/10GB` data are coming from the same place.  Briefly discuss the performance differences (if any) that you observed between the two data import methods, and why the differences might exist.
  - Compare the time to run queries for the TPCH 10GB dataset to your Redshift experiments.
  - Did any of the your queries require changes to run on Showflake, or were they identical to those you ran on Redshift?
  - Both Redshift and Snowflake have similar offerings and interface.  Briefly compare your experience with the two systems.  After using both, when would you choose to use Redshift over Snowflake?  Snowflake over Redshift?

## 5. Database Theory

Answer these two <a href="theory/hw2-theory.pdf">database theory questions</a> and submit them separately at the link provided.