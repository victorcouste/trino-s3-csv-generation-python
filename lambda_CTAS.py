from trino.dbapi import connect
from trino.auth import BasicAuthentication
import boto3
import datetime
import json

print('Loading function')

def lambda_handler(event, context):
    
    a = datetime.datetime.now()
    
    TRINO_CATALOG=event['catalog']
    CATALOG_SCHEMA=event['schema']
    TABLE=event['table']
    FILTER=event['filter']
    COLUMNS=event['columns']
    OUTPUT_FILE=event['output_file']
    OUTPUT_FOLDER=event['output_folder']
    AWS_S3_BUCKET=event['bucket']
    
    """ ---- For Tests ----- 
    {
      "catalog": "myglue",
      "schema": "myschema",
      "table": "mytable",
      "filter": "source_legal_entities='1' and parent_company='3'",
      "columns": "source_legal_entities,client_name,parent_company,product_name,fund_currency",
      "output_file": "demo_ctas",
      "output_folder": "output/",
      "bucket": "mydemobucket"
    }
    """

    #OUTPUT_FILE = "demo_ctas"
    #AWS_S3_BUCKET="mydemobucket"
    #SQL_QUERY="SELECT source_legal_entities,client_name,parent_company,product_name,fund_currency FROM mytable where source_legal_entities='1' and parent_company='3'"

    SQL_QUERY="SELECT "+COLUMNS+" FROM "+TABLE+" WHERE "+FILTER

    #print("Received event: " + json.dumps(event, indent=2))
    print("SQL query = " + SQL_QUERY)
    print("Output file = " + OUTPUT_FILE)
    print("S3 bucket = " + AWS_S3_BUCKET)    

    conn = connect(
        host='myaccount.trino.galaxy.cloud',
        port=443,
        user='your_email_account/accountadmin',
        catalog=TRINO_CATALOG,
        schema=CATALOG_SCHEMA,
        http_scheme='https', 
        auth=BasicAuthentication("your_email_account/accountadmin", "yourpassword"),
        session_properties={"scale_writers":"true", "writer_min_size":"1TB","task_writer_count":"1","myglue.compression_codec":"NONE"}
    )
    
    SQL_DROP="DROP TABLE IF EXISTS "+TRINO_CATALOG+".tmp."+OUTPUT_FILE
    SQL_CTAS="CREATE TABLE "+TRINO_CATALOG+".tmp."+OUTPUT_FILE+" WITH (csv_separator = ',',external_location='s3://"+AWS_S3_BUCKET+"/tmp/"+OUTPUT_FILE+"', format='csv') as "+SQL_QUERY

    cur = conn.cursor()

    cur.execute(SQL_CTAS)
    result=cur.fetchone()
    nbrows=json.dumps(result[0])
    
    cur.execute(SQL_DROP)
    cur.fetchone()
    
    cur.close()

    print(SQL_CTAS)
    print('CTAS query finished')
    print('Nb rows : '+nbrows)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_S3_BUCKET)
    
    for file in bucket.objects.filter(Prefix="tmp/"+OUTPUT_FILE+"/"):
        print(file.key)
        FROM_FILE=file.key
    
    #s3.Object(AWS_S3_BUCKET, "output/"+OUTPUT_FILE+".csv").copy_from(CopySource=AWS_S3_BUCKET+"/"+FROM_FILE)
    contents_data = bucket.Object(FROM_FILE).get()['Body'].read()
    content=b'\n'.join([bytes(COLUMNS.encode('utf-8')), contents_data])
    bucket.Object(OUTPUT_FOLDER+OUTPUT_FILE+".csv").put(Body=content)
    
    s3.Object(AWS_S3_BUCKET, FROM_FILE).delete()

    b = datetime.datetime.now()
    print(f"duration: {b-a}")

    value = {
        "dataset_url": "https://"+AWS_S3_BUCKET+".s3.eu-west-3.amazonaws.com/"+OUTPUT_FOLDER+OUTPUT_FILE+".csv",
        "nbrows": nbrows,
        "duration":str(b-a)
    }
    
    return value
    raise Exception('Something went wrong')
