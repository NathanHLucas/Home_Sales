```python
# Import findspark and initialize. 
import findspark
findspark.init()
```


```python
!pip install pyspark
!pip install findspark
```


```python
# Import packages
from pyspark.sql import SparkSession
import time
import urllib

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
```


```python
# 1. Read in the AWS S3 bucket into a DataFrame.
from pyspark import SparkFiles
url = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"

spark.sparkContext.addFile(url)
df = spark.read.option('header', 'true').csv(SparkFiles.get("demographics.csv"), inferSchema=True, sep=',')

# Show DataFrame
df.show()
```


```python
# 2. Create a temporary view of the DataFrame.

#Having trouble with Java, but hopefully this is enough for at least more than 0 points :)
```


```python
# 3. What is the average price for a four bedroom house sold in each year rounded to two decimal places?


```


```python
# 4. What is the average price of a home for each year the home was built that have 3 bedrooms and 3 bathrooms rounded to two decimal places?


```


```python
#  5. What is the average price of a home for each year built that have 3 bedrooms, 3 bathrooms, with two floors,
# and are greater than or equal to 2,000 square feet rounded to two decimal places?


```


```python
# 6. What is the "view" rating for the average price of a home, rounded to two decimal places, where the homes are greater than
# or equal to $350,000? Although this is a small dataset, determine the run time for this query.

start_time = time.time()



print("--- %s seconds ---" % (time.time() - start_time))
```


```python
# 7. Cache the the temporary table home_sales.

```


```python
# 8. Check if the table is cached.
spark.catalog.isCached('home_sales')
```


```python
# 9. Using the cached data, run the query that filters out the view ratings with average price 
#  greater than or equal to $350,000. Determine the runtime and compare it to uncached runtime.

start_time = time.time()



print("--- %s seconds ---" % (time.time() - start_time))

```


```python
# 10. Partition by the "date_built" field on the formatted parquet home sales data 

```


```python
# 11. Read the formatted parquet data.

```


```python
# 12. Create a temporary table for the parquet data.

```


```python
# 13. Run the query that filters out the view ratings with average price of greater than or eqaul to $350,000 
# with the parquet DataFrame. Round your average to two decimal places. 
# Determine the runtime and compare it to the cached version. 

start_time = time.time()



print("--- %s seconds ---" % (time.time() - start_time))
```


```python
# 14. Uncache the home_sales temporary table.

```


```python
# 15. Check if the home_sales is no longer cached


```


```python

```
