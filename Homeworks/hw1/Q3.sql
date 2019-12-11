-- Resize the nodes to 4 before running.
-- 1.1
-- What is the total number of parts offered by each supplier? 
-- The query should return the name of the supplier and the total number of parts.
SELECT s.s_name AS supplier_name, SUM(ps.ps_availqty) AS Total_part_num
FROM supplier AS s, partsupp AS ps
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
FROM partsupp AS ps, part AS p
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
FROM supplier AS s, partsupp AS ps, part AS p
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
FROM customer AS c, nation AS n
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
FROM lineitem AS l, supplier AS s
WHERE l.l_suppkey= s.s_suppkey AND (l.l_shipdate >= '1996-10-10 00:00:00' 
AND l.l_shipdate <  '1996-11-11 00:00:00')
GROUP BY supplier_name;

-- Check Runtime
SELECT (endtime - starttime) AS runtime, starttime
FROM stl_query
where stl_query.querytxt LIKE 
'SELECT s.s_name AS supplier_name, SUM(l.l_quantity) AS num_of_parts %'
ORDER BY starttime DESC;
