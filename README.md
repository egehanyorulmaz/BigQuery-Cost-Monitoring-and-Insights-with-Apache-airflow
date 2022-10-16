## BigQuery Cost Monitoring with Apache Airflow

### **Problem**
Thousands of businesses prefer BigQuery as data warehouse in their data pipeline. In the lack of data engineer resources, 
BigQuery Data Transfer Service becomes useful since it offers a variety of sources to integrate to BigQuery like Google 
Products (Youtube, Google Analytics, Cloud Resources), some AWS products (Redshift, S3), and hundreds of 3rd party 
transfer tools. After a while, you figure out that the majority of costs billed to you for Bigquery comes from
Analysis, not from Storage. At that point, you might decide to find ways of reducing the analysis cost and
you find some articles about query optimization in BigQuery. You gave few bullet points that has to be avoided in BigQuery SQL like 
"Limiting doesn't reduce the bytes processed", "Always SELECT the columns you are interested in". But, you still don't
observe any decrease in the billing. At that point, **this repository comes to play** :)


### **Approach and Solution**
My approach to the problem was to first understand the root cause of the problem. What is the major cost in BigQuery? 
Is it high because of the queries run by users or automated jobs? What tables and views are contributing to the cost? 
How do we prioritize the views to optimize?

Using the [BigQuery Module](https://googleapis.dev/python/bigquery/latest/index.html), we can access, manage and interact 
with the element of BigQuery. Within the module, there is bigquery.job class that is used to access the history and details 
about every query and load jobs run in BigQuery. 

**1- BigQuery Class**\
Creates a BigQuery client with the google credential path of json file.

**2- BigQueryJobs Class**\
Inheriting from the BigQuery class and uses the client to list the historical job records and iterates over the jobs
to extract the following details:

<ol>
  <li>Job Type (Load or Query)</li>
  <li>Creation Time</li>
  <li>User Email</li>
  <li>Query</li>
  <li>Query run time</li>
  <li>Billed bytes for the query run</li>
</ol>

To understand which tables are used in the query, I used the Parser method from sql_metadata Python module.
sql_metadata library does the following:

**SQL Query**: `SELECT * FROM table1 t1\
LEFT JOIN table2 t2\
ON t1.x=t2.x`\
**sql_metadata output:**
`["table1", "table2"]`

So, we can apply the same logic to BigQuery queries and extract the tables inside the query. Then, assuming that every 
table in the query contributes the same amount of processed bytes, for each query we can calculate the total billed bytes per table.\
![image](https://user-images.githubusercontent.com/48676337/196013075-09d120b2-2735-4185-abac-6e9c4fac30da.png)


Now, since we know the rough estimate of bytes per table we can use the [BigQuery Pricing based on region and currency](https://cloud.google.com/skus/?currency=USD&filter=bigquery+analysis), and calculate 
the total cost per query by processing all the data returned by the client. 

The rest of the job is to schedule this code using Apache Airflow! It has already been implemented in this code :). 

To use the code:
You have to replace your appropriate credentials to XXXXXX in SqlQueryManager.py



