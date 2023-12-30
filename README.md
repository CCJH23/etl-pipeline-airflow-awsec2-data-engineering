# Python ETL Pipeline with Airflow and AWS EC2 Data Engineering Project

Python ETL pipeline with Airflow on AWS EC2 Data Engineering Project

[Architecture Diagram]

[DAG Graph]

Process

1. Download Airflow in Amazon EC2
2. SSH into EC2 to launch Airflow and create DAGs
3. Create a DAG in Airflow
   1. Create a task to check if the OpenWeatherMap API is accessable first to prevent the DAG from failing if the API is down or the endpoint is wrong
   2. Create a task to extract the weather data from the OpenWeatherMap API, providing the city and API key
   3. Create a task to transform the data, convert into Pandas dataframe and store the data in s3 bucket as a csv file
4. Create S3 bucket
5. Modify IAM permissions for EC2 so that it can access S3 bucket
   1. Create role with AmazonS3fullaccess and Amazonec2fullaccess
6. Get AWS access key so that the DAG has permissions to run
   1. Create access key in AWS security credentials to get access key and secret access key
   2. Install awscli in EC2
   3. Set up aws configure to provide the access key and secret access key
   4. Get an aws session token
   5. Form the aws credentials with access key, secret access key and session token as a dictionary and provide this dictionary as an argument when saving the data frame to csv in the s3 bucket
