import boto3
import json

def create_aws_file(bucket,catalog,schema,table,filter,columns,output_folder,output_file,lambda_function):

	payload={
	  "bucket": bucket,
	  "catalog": catalog,
	  "schema": schema,
	  "table": table,
	  "filter": filter,
	  "columns": columns,
	  "output_file": output_file,
	  "output_folder": output_folder
	}

	print("Test with "+lambda_function+" on "+json.dumps(payload,indent=1))

	client = boto3.client('lambda', region_name='eu-west-3')

	response = client.invoke(
	    FunctionName=lambda_function,
	    InvocationType='RequestResponse',
	    Payload=bytes(json.dumps(payload).encode('utf-8'))
	)

	json_result = json.loads(response['Payload'].read())

	print("Lambda run result : "+json.dumps(json_result))

	return json_result