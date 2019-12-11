-- A customer is considered a Gold customer if they have orders totalling more than $1,000,000.00. 
-- Customers with orders totalling between $1,000,000.00 and $500,000.00 are considered Silver. 
-- Write a SQL query to compute the number of customers in these two categories. 
-- Try different methods of writing the query (only SQL or use a UDF or a View to categorize a user). 
-- Discuss your experience with the various methods to carry out such analysis. 
-- Use the 1GB data set and the 2-node cluster. (10 points)

-- Only SQL Method
SELECT Category, COUNT(customer) as num_of_customer 
FROM (
  SELECT CASE WHEN SUM( o_totalprice )> 1000000.00 THEN 'gold_member'
    WHEN SUM(o_totalprice ) <= 1000000.00 AND SUM(o_totalprice ) > 500000.00 THEN 'silver_member'
    ELSE NULL
   END AS Category, c_custkey AS customer
   FROM orders, customer
   WHERE o_custkey = c_custkey
   GROUP BY customer)
WHERE Category is not NULL
GROUP BY Category;


-- Create a view method
DROP VIEW membership;
CREATE VIEW membership AS
  SELECT c_custkey AS customer,
  CASE WHEN SUM(o_totalprice) > 1000000.00 THEN 'gold_member'
  WHEN SUM(o_totalprice) > 500000.00 AND SUM(o_totalprice) <= 1000000.00 THEN 'silver_member'
  ELSE NULL
  END AS Category
FROM customer, orders
WHERE o_custkey = c_custkey
GROUP BY c_custkey;
  
SELECT Category, COUNT(customer) AS number_of_customer
FROM membership
WHERE Category is not NULL
GROUP BY Category;

-- Check Runtime
SELECT (endtime - starttime) AS runtime, starttime
FROM stl_query
where stl_query.querytxt LIKE 
'SELECT Category, COUNT(customer) AS number_of_customer %'
ORDER BY starttime DESC;

-- UDF method

Drop FUNCTION  membership(float);
CREATE FUNCTION membership(float)
  returns VARCHAR
stable
AS $$
  SELECT CASE WHEN $1 > 1000000.00 THEN 'gold_member'
    WHEN $1 > 500000.00 AND $1 <= 1000000.00 THEN 'silver_member'
    ELSE NULL
    END
$$ language sql;

SELECT Category, COUNT(customer) AS num_of_customer 
FROM (SELECT membership(SUM(o_totalprice)) AS Category, c_custkey AS customer
   FROM orders, customer
   WHERE o_custkey = c_custkey
   GROUP BY customer)
WHERE Category IS NOT NULL
GROUP BY Category;


-- Check Runtime
SELECT (endtime - starttime) AS runtime, starttime
FROM stl_query
where stl_query.querytxt LIKE 
'SELECT Category, COUNT(customer) AS num_of_customer %'
ORDER BY starttime DESC;
