# Data Lake (Spark)

## Project
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Here an ETL pipeline is built that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Data
The data is distributed over two datasets that reside in S3. Here are the S3 links for each:

Song data: `s3://udacity-dend/song_data`
Log data: `s3://udacity-dend/log_data`

Log data json path: `s3://udacity-dend/log_json_path.json`

## How to run
### Locally
Remember to change the input/output folder in `dl.cfg`.
```bash
pip install -r requirements.txt
python etl.py
```
This will output parquet files to `./analytics/`

### EMR cluster
Set up an EMR cluster (see section below).

Then submit etl.py to the spark cluster.

An example of that,
`/usr/bin/spark-submit --master yarn ./etl.py` when connected to the master node.

## How to spin up resources
### Using AWS Cli
**Prerequisite**
1. [Install AWS Cli](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
2. Set up Access credentials using AWS IAM
3. Create EC2 Login Key-Pair - You should have an EC2 login key-pair to access your EC2 instances in the cluster.

**Create an EMR Cluster**
1. Create default roles in IAM - `../Exercices/EMRSetup/create-roles.sh`
2. Launch your cluster - ` ../Exercices/EMRSetup/create-cluster.sh -c 'test-cluster' -b <bootstrap-filepath> -k <ec2-key-pair>`
3. Change the inbound rule for the master security group to allow for your IP.

**Set up dynamic port forwarding**

`ssh -i <EC2-KEY-PAIR> -N -D 8157 hadoop@ec2-<Master-node-ip>.us-east-2.compute.amazonaws.com`