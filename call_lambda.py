import boto3
import json

client = boto3.client('lambda', region_name='eu-west-3')

#SQL="SELECT source_legal_entities,client_name,parent_company,product_name,fund_currency FROM mytable where source_legal_entities='1' and parent_company='3'"

BUCKET="mydemobucket"
FOLDER="output/"
CATALOG="myglue"
SCHEMA="myschema"
TABLE="mytable"
COLUMNS="source_legal_entities,client_name,parent_company,product_name,fund_currency"
FILTER="source_legal_entities='1' and parent_company='3'"

payload={
  "catalog": CATALOG,
  "schema": SCHEMA,
  "table": TABLE,
  "filter": FILTER,
  "columns": COLUMNS,
  "output_file": "file_output_CTAS",
  "output_folder":FOLDER,
  "bucket": BUCKET
}

print("\nCTAS test ...")

response = client.invoke(
    FunctionName="Lambda-CTAS",
    InvocationType='RequestResponse',
    Payload=bytes(json.dumps(payload).encode('utf-8'))
)
print("CTAS result : "+json.dumps(json.loads(response['Payload'].read()),indent=2))
#print("CTAS result : "+str(response['Payload'].read()))

payload={
  "catalog": CATALOG,
  "schema": SCHEMA,
  "table": TABLE,
  "filter": FILTER,
  "columns": COLUMNS,
  "output_file": "file_output_Pandas",
  "output_folder":FOLDER,
  "bucket": BUCKET
}

print("\nPandas test ...")

client = boto3.client('lambda', region_name='eu-west-3')

response = client.invoke(
    FunctionName="Lambda-Pandas",
    InvocationType='RequestResponse',
    Payload=bytes(json.dumps(payload).encode('utf-8'))
)
print("Pandas result : "+json.dumps(json.loads(response['Payload'].read()),indent=2))
