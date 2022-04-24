# CSV files generation from Trino with AWS Lambda

This repository contains 2 examples of [Python AWS Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/lambda-python.html) to generate a single CSV file in a S3 bucket from a Trino SQL query result.

Tests and examples are done with [Galaxy](https://www.starburst.io/platform/starburst-galaxy/), the Trino SaaS version, but it's the same for a hosted [Trino](https://www.trino.io) or [Starburst Enterprise](https://www.starburst.io/platform/starburst-enterprise) cluster.

We describe here 2 options to generate CSV files:

- From a CTAS (Create table query) query
- Using a Python Pandas DataFrame object

And to run these AWS Lambda functions, you will also need to add this [layer package](https://github.com/victorcouste/trino-s3-csv-generation-python/blob/main/pandas_trino_layer.zip) in order to use [Trino Python library](https://github.com/trinodb/trino-python-client) and [Python pandas](https://pandas.pydata.org/) objects ([about Lambda layer](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-concepts.html#gettingstarted-concepts-layer)).

You can find also a [Python code to test](https://github.com/victorcouste/trino-s3-csv-generation-python/blob/main/call_lambda.py) these functions and a Flask Web application using these functions.

---

## Function with a CTAS query

The [lambda_CTAS.py](https://github.com/victorcouste/trino-s3-csv-generation-python/blob/main/lambda_CTAS.py) function uses a CTAS query to generate a single CSV file in a S3 bucket.

The CTAS query is like:

`CREATE TABLE s3_catalog.tmp.your_file WITH (csv_separator = ',',external_location='s3://your_bucket/tmp/your_file', format='csv') as SELECT ....`

Where **tmp** is an existing Schema in your Trino or Galaxy S3 Catalog (Glue or Hive), here named **s3_catalog**.

The extra steps into the function after the CTAS query run are to:
- Add .csv suffix to the file name
- Add columns name as header (from Columns name passed as function parameters)
- Copy the file in your specific bucket folder
- Remove the temporary table created (from the CTAS)

**Important**, you need also to set some properties to the session in order to generate only 1 file not compressed.

session_properties:
- scale_writers : true
- writer_min_size : 1TB
- task_writer_count : 1
- s3_catalog.compression_codec : NONE


## Function with a Pandas DataFrame

For the [lambda_pandas.py](https://github.com/victorcouste/trino-s3-csv-generation-python/blob/main/lambda_pandas.py) function, we just load the SQL query output in a Pandas DataFrame object, and then we generate a CSV file in a bucket folder from the DataFrame.

No specific session properties need to be set but the query result must fit in-memory.

## Flask application

In the [s3-file-application folder](https://github.com/victorcouste/trino-s3-csv-generation-python/tree/main/s3-file-application) you can find a [Flask](https://palletsprojects.com/p/flask/) Web application using the 2 Lambda functions.

![Flask application](https://github.com/victorcouste/trino-s3-csv-generation-python/blob/main/flask-application.png?raw=true)

---

Finally, you can connect to another existing [Trino](https://www.trino.io) or [Starburst Enterprise](https://www.starburst.io/platform/starburst-enterprise) (Trino enterprise edition) deployment or cluster for more scalability, security and performance.

Have fun!