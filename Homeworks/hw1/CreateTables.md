# Create Tables in Amazon Redshift

```sql
CREATE TABLE customer(
C_CustKey int ,
C_Name varchar(64) ,
C_Address varchar(64) ,
C_NationKey int ,
C_Phone varchar(64) ,
C_AcctBal decimal(13, 2) ,
C_MktSegment varchar(64) ,
C_Comment varchar(120) ,
skip varchar(64)
);

CREATE TABLE lineitem(
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
skip varchar(64)
);

CREATE TABLE nation(
N_NationKey int ,
N_Name varchar(64) ,
N_RegionKey int ,
N_Comment varchar(160) ,
skip varchar(64)
);

CREATE TABLE orders(
O_OrderKey int ,
O_CustKey int ,
O_OrderStatus varchar(64) ,
O_TotalPrice decimal(13, 2) ,
O_OrderDate datetime ,
O_OrderPriority varchar(15) ,
O_Clerk varchar(64) ,
O_ShipPriority int ,
O_Comment varchar(80) ,
skip varchar(64)
);

CREATE TABLE part(
P_PartKey int ,
P_Name varchar(64) ,
P_Mfgr varchar(64) ,
P_Brand varchar(64) ,
P_Type varchar(64) ,
P_Size int ,
P_Container varchar(64) ,
P_RetailPrice decimal(13, 2) ,
P_Comment varchar(64) ,
skip varchar(64)
);

CREATE TABLE partsupp(
PS_PartKey int ,
PS_SuppKey int ,
PS_AvailQty int ,
PS_SupplyCost decimal(13, 2) ,
PS_Comment varchar(200) ,
skip varchar(64)
);

CREATE TABLE region(
R_RegionKey int ,
R_Name varchar(64) ,
R_Comment varchar(160) ,
skip varchar(64)
);

CREATE TABLE supplier(
S_SuppKey int ,
S_Name varchar(64) ,
S_Address varchar(64) ,
S_NationKey int ,
S_Phone varchar(18) ,
S_AcctBal decimal(13, 2) ,
S_Comment varchar(105) ,
skip varchar(64)
);
```


# Copy TPC-H Data from s3 to Redshift

Use the section instructions to load the 1GB dataset and the following instructions to load the 10GB dataset.   In the assignment, some questions use the 1GB dataset and others the 10GB one.

```sql
copy customer from 's3://uwdb/tpch/uniform/10GB/customer.tbl' CREDENTIALS 'aws_iam_role=<your redshift role arn>' delimiter '|';
copy orders from 's3://uwdb/tpch/uniform/10GB/orders.tbl' REGION 'us-west-2' CREDENTIALS 'aws_iam_role=<your redshift role arn>' delimiter '|';
copy lineitem from 's3://uwdb/tpch/uniform/10GB/lineitem.tbl'REGION 'us-west-2' CREDENTIALS 'aws_iam_role=<your redshift role arn>' delimiter '|';
copy nation from 's3://uwdb/tpch/uniform/10GB/nation.tbl'REGION 'us-west-2' CREDENTIALS 'aws_iam_role=<your redshift role arn>' delimiter '|';
copy part from 's3://uwdb/tpch/uniform/10GB/part.tbl'REGION 'us-west-2' CREDENTIALS 'aws_iam_role=<your redshift role arn>' delimiter '|';
copy partsupp from 's3://uwdb/tpch/uniform/10GB/partsupp.tbl'REGION 'us-west-2' CREDENTIALS 'aws_iam_role=<your redshift role arn>' delimiter '|';
copy region from 's3://uwdb/tpch/uniform/10GB/region.tbl'REGION 'us-west-2' CREDENTIALS 'aws_iam_role=<your redshift role arn>' delimiter '|';
copy supplier from 's3://uwdb/tpch/uniform/10GB/supplier.tbl'REGION 'us-west-2' CREDENTIALS 'aws_iam_role=<your redshift role arn>' delimiter '|';
```