
from flask import Flask, request,render_template,Markup
from aws_lambda_function import create_aws_file

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def home():

    bucket= "mybucket"
    catalog= "myglue"
    schema= "myschema"
    table=""
    filter="source_legal_entities='1' and parent_company='3'"
    columns="source_legal_entities,client_name,parent_company,product_name,fund_currency"
    output_file="demofile"
    output_folder="output/"
    lambda_function=""

    file_creation_result={}

    if request.method == 'POST':
        table=request.form['table']
        filter=request.form['filter']
        columns=request.form['columns']
        output_file=request.form['output_file']
        output_folder=request.form['output_folder']
        lambda_function=request.form['lambda_function']
        file_creation_result=create_aws_file(bucket,catalog,schema,table,filter,columns,output_folder,output_file,lambda_function)

    return render_template('home.html',file_creation_result=file_creation_result,catalog=catalog,schema=schema,bucket=bucket,table=table,filter=filter,columns=columns,output_folder=output_folder,output_file=output_file,lambda_function=lambda_function)