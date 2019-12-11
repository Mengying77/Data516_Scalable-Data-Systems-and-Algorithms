-- Query data on s3 vs local data: re-run the queries from (1) on the 10GB data set on s3. 
-- Data is located in s3://uwdb/tpch/athena/ with a folder for each of the following tables: 
-- customer , supplier , orders , region , nation , part , partsupp & lineitem . 
-- The data is in textfile format and the delimiter is the pipe character (|). 
-- For example, the s3 location of the data for the lineitem relation is s3://uwdb/tpch/athena/linitem/ 

-- Creating external database connection
create external schema tpchs3
from data catalog
database 'tpchs3'
iam_role 'arn:aws:iam::295193264972:role/RedshiftRole'
create external database if not exists;       

create external table tpchs3.customer(
C_CustKey int ,
C_Name varchar(64) ,
C_Address varchar(64) ,
C_NationKey int ,
C_Phone varchar(64) ,
C_AcctBal decimal(13, 2) ,
C_MktSegment varchar(64) ,
C_Comment varchar(120) ,
skip varchar(64))
row format delimited
fields terminated by '|'
stored as textfile
location 's3://uwdb/tpch/athena/customer/'
table properties ('numRows'='1500000');

create external table tpchs3.lineitem(
  L_OrderKey int ,
  L_PartKey int ,
  L_SuppKey int ,
  L_LineNumber int ,
  L_Quantity int ,
  L_ExtendedPrice decimal(13, 2) ,
  L_Discount decimal(13, 2) ,
  L_Tax decimal(13, 2) ,
  L_ReturnFlag varchar(64) ,
  L_LineStatus varchar(64) ,
  L_ShipDate datetime ,
  L_CommitDate datetime ,
  L_ReceiptDate datetime ,
  L_ShipInstruct varchar(64) ,
  L_ShipMode varchar(64) ,
  L_Comment varchar(64) ,
  skip varchar(64))
  row format delimited
  fields terminated by '|'
  stored as textfile
  location 's3://uwdb/tpch/athena/lineitem/'
  table properties ('numRows'='65984650');

create external table tpchs3.nation(
  N_NationKey int ,
  N_Name varchar(64) ,
  N_RegionKey int ,
  N_Comment varchar(160) ,
  skip varchar(64))
  row format delimited
  fields terminated by '|'
  stored as textfile
  location 's3://uwdb/tpch/athena/nation/'
  table properties ('numRows'='25');

create external table tpchs3.orders(
  O_OrderKey int ,
  O_CustKey int ,
  O_OrderStatus varchar(64) ,
  O_TotalPrice decimal(13, 2) ,
  O_OrderDate datetime ,
  O_OrderPriority varchar(15) ,
  O_Clerk varchar(64) ,
  O_ShipPriority int ,
  O_Comment varchar(80) ,
  skip varchar(64))
  row format delimited
  fields terminated by '|'
  stored as textfile
  location 's3://uwdb/tpch/athena/orders/'
  table properties ('numRows'='15000000');

create external table tpchs3.part(
  P_PartKey int ,
  P_Name varchar(64) ,
  P_Mfgr varchar(64) ,
  P_Brand varchar(64) ,
  P_Type varchar(64) ,
  P_Size int ,
  P_Container varchar(64) ,
  P_RetailPrice decimal(13, 2) ,
  P_Comment varchar(64) ,
  skip varchar(64))
  row format delimited
  fields terminated by '|'
  stored as textfile
  location 's3://uwdb/tpch/athena/part/'
  table properties ('numRows'='2000000');

create external table tpchs3.partsupp(
  PS_PartKey int ,
  PS_SuppKey int ,
  PS_AvailQty int ,
  PS_SupplyCost decimal(13, 2) ,
  PS_Comment varchar(200) ,
  skip varchar(64))
  row format delimited
  fields terminated by '|'
  stored as textfile
  location 's3://uwdb/tpch/athena/partsupp/'
  table properties ('numRows'='8000000');

create external table tpchs3.region(
  R_RegionKey int ,
  R_Name varchar(64) ,
  R_Comment varchar(160) ,
  skip varchar(64))
  row format delimited
  fields terminated by '|'
  stored as textfile
  location 's3://uwdb/tpch/athena/region/'
  table properties ('numRows'='5');

create external table tpchs3.supplier(
  S_SuppKey int ,
  S_Name varchar(64) ,
  S_Address varchar(64) ,
  S_NationKey int ,
  S_Phone varchar(18) ,
  S_AcctBal decimal(13, 2) ,
  S_Comment varchar(105) ,
  skip varchar(64))
  row format delimited
  fields terminated by '|'
  stored as textfile
  location 's3://uwdb/tpch/athena/supplier/'
  table properties ('numRows'='100000');
  
-- 1.1
-- What is the total number of parts offered by each supplier? 
-- The query should return the name of the supplier and the total number of parts.
SELECT s.s_name AS supplier_name, SUM(ps.ps_availqty) AS Total_part_num
FROM tpchs3.supplier AS s, tpchs3.partsupp AS ps
WHERE ps.ps_suppkey = s.s_suppkey
GROUP BY supplier_name;

-- Check Runtime
SELECT (endtime - starttime) AS runtime, querytxt, starttime, endtime
FROM stl_query
where stl_query.querytxt LIKE 
'SELECT s.s_name AS supplier_name, SUM(ps.ps_availqty) AS Total_part_num%'
ORDER BY starttime DESC;

-- 1.2
-- What is the cost of the most expensive part by any supplier?
-- The query should return only the price of that most expensive part. No need to return the name.
SELECT MAX(p.p_retailprice) AS max_price
FROM tpchs3.partsupp AS ps, tpchs3.part AS p
WHERE ps.ps_partkey = p.p_partkey;

-- Check Runtime
SELECT (endtime - starttime) AS runtime, starttime
FROM stl_query
where stl_query.querytxt LIKE 
'SELECT MAX(p.p_retailprice) AS max_price%'
ORDER BY starttime DESC;

-- 1.3
-- What is the cost of the most expensive part for each supplier? 
-- The query should return the name of the supplier and the cost of the most
-- expensive part but you do not need to return the name of that part.
SELECT s.s_name AS supplier_name, MAX(p.p_retailprice) AS max_price 
FROM tpchs3.supplier AS s, tpchs3.partsupp AS ps, tpchs3.part AS p
WHERE ps.ps_suppkey = s.s_suppkey AND ps.ps_partkey = p.p_partkey
GROUP BY supplier_name;

-- Check Runtime
SELECT (endtime - starttime) AS runtime, starttime
FROM stl_query
where stl_query.querytxt LIKE 
'SELECT s.s_name AS supplier_name, MAX(p.p_retailprice) AS max_price %'
ORDER BY starttime DESC;

-- 1.4
-- What is the total number of customers per nation? 
-- The query should return the name of the nation and the number of unique customers.
SELECT n.n_name AS nation, count(c.c_custkey) AS total_customer_num
FROM tpchs3.customer AS c, tpchs3.nation AS n
WHERE n.n_nationkey = c.c_nationkey
GROUP BY nation;

-- Check Runtime
SELECT (endtime - starttime) AS runtime, starttime
FROM stl_query
where stl_query.querytxt LIKE 
'SELECT n.n_name AS nation, count(c.c_custkey) AS total_customer_num%'
ORDER BY starttime DESC;

-- 1.5
-- What is number of parts shipped between 10 oct, 1996 and 10 nov, 1996 for each supplier? 
-- The query should return the name of the supplier and the number of parts
SELECT s.s_name AS supplier_name, SUM(l.l_quantity) AS num_of_parts
FROM tpchs3.lineitem AS l, tpchs3.supplier AS s
WHERE l.l_suppkey= s.s_suppkey AND (l.l_shipdate >= '1996-10-10 00:00:00' 
AND l.l_shipdate <  '1996-11-11 00:00:00')
GROUP BY supplier_name;

-- Check Runtime
SELECT (endtime - starttime) AS runtime, starttime
FROM stl_query
where stl_query.querytxt LIKE 
'SELECT s.s_name AS supplier_name, SUM(l.l_quantity) AS num_of_parts %'
ORDER BY starttime DESC;

