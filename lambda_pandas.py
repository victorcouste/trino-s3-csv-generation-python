import io
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import boto3
import datetime

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

''' ---- For tests    
    {
  "catalog": "myglue",
  "schema": "myschema",
  "table": "mytable",
  "filter": "source_legal_entities='1' and parent_company='3'",
  "columns": "source_legal_entities,client_name,parent_company,product_name,fund_currency",
  "output_file": "demo_pandas",
  "output_folder": "output/",
  "bucket": "mybucket"
}
'''
    SQL_QUERY="SELECT "+COLUMNS+" FROM "+TABLE+" WHERE "+FILTER
    
    #print("Received event: " + json.dumps(event, indent=2))
    print("SQL query = " + SQL_QUERY)
    print("Output file name = " + OUTPUT_FILE)
    print("S3 bucket name = " + AWS_S3_BUCKET)    

    conn = connect(
        host='myaccount.trino.galaxy.cloud',
        port=443,
        user='your_email_account/accountadmin',
        catalog=TRINO_CATALOG,
        schema=CATALOG_SCHEMA,
        http_scheme='https', 
        auth=BasicAuthentication("your_email_account/accountadmin", "yourpassword"),
    )
    
    cur = conn.cursor()
    cur.execute(SQL_QUERY)
    rows = cur.fetchall()
    colnames = [part[0] for part in cur.description]
    nbrows = str(len(rows))
    cur.close()
    
    #print('Trino result : {}'.format(rows))
    print('Trino query finished')
    print('Nb rows : '+nbrows)

    data_df = pd.DataFrame(rows, columns=colnames)

    s3_client = boto3.client('s3')

    with io.StringIO() as csv_buffer:
    
        data_df.to_csv(csv_buffer, index=False)
    
        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET, Key=OUTPUT_FOLDER+OUTPUT_FILE+".csv", Body=csv_buffer.getvalue()
        )
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
    
    b = datetime.datetime.now()
    print(f"duration: {b-a}")

    value = {
        "dataset_url": "https://"+AWS_S3_BUCKET+".s3.eu-west-3.amazonaws.com/"+OUTPUT_FOLDER+OUTPUT_FILE+".csv",
        "nbrows": nbrows,
        "duration":str(b-a)
    }
    
    return value
    raise Exception('Something went wrong')