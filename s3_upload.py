import boto3

s3 = boto3.client('s3')
s3.upload_file('products.csv','databricks-exercise','products.csv')
s3.upload_file('sales.csv','databricks-exercise','sales.csv')
s3.upload_file('sellers.csv','databricks-exercise','sellers.csv')