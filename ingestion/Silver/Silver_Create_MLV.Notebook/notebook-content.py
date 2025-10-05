# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2e7941c7-46a4-4cd6-a131-edb5b42603ce",
# META       "default_lakehouse_name": "Silver",
# META       "default_lakehouse_workspace_id": "64abe8bb-34b3-4d9a-b901-f47a9eb3359a",
# META       "known_lakehouses": [
# META         {
# META           "id": "2e7941c7-46a4-4cd6-a131-edb5b42603ce"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS Silver.dbo.silver_customer
# MAGIC (
# MAGIC CONSTRAINT name_check CHECK(customername != '') ON MISMATCH FAIL
# MAGIC )
# MAGIC AS
# MAGIC SELECT 
# MAGIC cust.accountnum,
# MAGIC cust.custgroup,
# MAGIC cust.dlvterm,
# MAGIC cust.party,
# MAGIC custgroup.name as customergroup,
# MAGIC COALESCE(dirparty.name, '') as customername
# MAGIC FROM Bronze.dbo.custtable cust
# MAGIC INNER JOIN Bronze.dbo.custgroup custgroup ON custgroup.custgroup = cust.custgroup and custgroup.dataareaid = cust.dataareaid
# MAGIC INNER JOIN Bronze.dbo.dirpartytable dirparty ON dirparty.recid = cust.party
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE MATERIALIZED LAKE VIEW IF NOT EXISTS Silver.dbo.silver_sales_invoice
# MAGIC (
# MAGIC CONSTRAINT name_check CHECK(customername != '') ON MISMATCH FAIL
# MAGIC )
# MAGIC AS
# MAGIC SELECT 
# MAGIC cust.accountnum,
# MAGIC cust.custgroup,
# MAGIC cust.dlvterm,
# MAGIC cust.party,
# MAGIC custgroup.name as customergroup,
# MAGIC COALESCE(dirparty.name, '') as customername
# MAGIC FROM Bronze.dbo.custinvoicejour cij
# MAGIC INNER JOIN Bronze.dbo.custinvoicetrans cit ON cij.invoiceid = cit.invoiceid and cif.transdate = cit.transdate and cij.dataareaid = cit.dataareaid
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select invoiceid, invoicedate, orderaccount, invoiceaccount, currencycode, ledgervoucher, invoiceamount, invoiceamountmst, sumtax, sumtaxmst, dataareaid from bronze.dbo.custinvoicejour cij

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select invoiceid, invoicedate, currencycode, salesid, *, dataareaid from bronze.dbo.custinvoicetrans cij

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
