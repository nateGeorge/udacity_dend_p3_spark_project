# Sparkify Project

## Summary of the project
This project takes a collection of json log files on AWS S3, loads them into spark, and creates a star-schema database stored in Parquet files.

## How to run the Python scripts

Copy Udacity S3 data to your own bucket may speed up reads:

`aws s3 sync s3://udacity-dend s3://dend-data`

However, it takes a long time since every single song json data is in a separate file.

There are a few ways to run this.  All require that AWS credentials are set in `dl.cfg`.  I used administrator credentials, but anything that allows reading/writing from your private buckets works.

I referenced [this post](https://stackoverflow.com/a/39984117/4549682) for how to write to S3 faster.

### Udacity workspace notebook
One is with the demo files from the workspace.  The notebook `workspace_prototyping.ipynb` can be run from the Udacity workspace with the demo file.  This will do everything: unzip data files, load them, and write data to an S3 bucket.  The S3 bucket for writing output parquet files should be created and the value of `output_data` changed in the notebook.

### EMR notebook
Another way is to run the notebook `udacity-p3-sparkify-emr.ipynb` on an EMR cluster.  I used a 3-node cluster with m5.xlarge instances.  If the EMR cluster is set up in the same region and with default security parameters, you should be able to access your S3 buckets without the .cfg file.

### EMR command line
You should click on the cluster, then go to the security groups link for the master node.  Then you shoul allow SSH from your IP in incoming connections.  You should've created the cluster with an AWS EC2 key pair which you can use to SSH.  Or click on the SSH link next to 'Master Public DNS' in the cluster description.

SCP the file to the instance: `scp -i keypair.pem hadoop@instanceaddress:/home/hadoop/ etl.py`.  Then SSH into the instance.  Next, run the command `spark-submit --master yarn etl.py`.  You shouldn't need the .cfg file if you have default permissions for the cluster and it's in the same region as the S3 buckets.

Any of these methods take a long, long time to run.  Most of the time seems to be spent writing the data to S3.

### Best practices for writing data
Writing data as parquet files to S3 is extremely slow.  There is a setting in the hadoop configs that can be set from spark to increase speed, but it's still 10x slower than writing to HDFS.  So it seems it's best to write to HDFS then copy the data to S3 with `s3-dist-cp` or `hadoop distcp`:

`s3-dist-cp --src hdfs:///dwh/songs --dest s3://udacity-dend-spark-dwh3/songs`
`s3-dist-cp --src hdfs:///dwh/artists --dest s3://udacity-dend-spark-dwh3/artists`
`s3-dist-cp --src hdfs:///dwh/users --dest s3://udacity-dend-spark-dwh3/users`
`s3-dist-cp --src hdfs:///dwh/time --dest s3://udacity-dend-spark-dwh3/time`
`s3-dist-cp --src hdfs:///dwh/songplays --dest s3://udacity-dend-spark-dwh3/songplays`

This could be done in a few lines using a loop instead of 5 separate lines, or put in a bash script, like the `copy_to_s3.sh` file here.

or

`hadoop distcp hdfs:///dwh/songs s3a://udacity-dend-spark-dwh3/songs`

and so on.

#### s3-dist-cp bug
For some reason, when creating the default cluster with the spark software setup, it doesn't install `s3-dist-cp`.  I found that something in this combination of software (using the 'advanced' cluster setup mode) enables s3-dist-cp:

Ganglia 3.7.2, Hive 2.3.6, Hue 4.4.0, Mahout 0.13.0, Pig 0.17.0, Tez 0.9.2, Spark

Also in the advanced mode you have the chance to use spot instances instead of on-demand instances, which saves a lot of dough.

## Files in the repository
`etl.py` -- this is a Python script that will load the log and song JSON files into spark, create the data for star schema tables, and write it to S3 as parquet files.

`dl.cfg` -- a config file where you should set AWS credentials

`log-data.zip` and `song-data.zip` -- zip files with JSON sample data

`udacity-p3-sparkify-emr.ipynb` -- jupyter notebook for running the same thing as `etl.py` on EMR

`workspace_prototyping.ipynb` -- jupyter notebook for running the same thing as `etl.py` on the Udacity workspace

`copy_to_s3.sh` -- copies files from HDFS to S3 bucket.  Writing parquet to HDFS then copying with s3-dist-copy to S3 is much faster than writing parquet files directly to the S3 bucket.  Run `time ./copy_to_s3.sh` to see how long the process takes (after making it executable with `sudo chmod a+x copy_to_s3.sh`)

## Other discussion

1. The purpose of this database in context of the startup, Sparkify, and their analytical goals.

This database is to take log data from users playing songs on Sparkify's app and be able to analyze them for business purposes with a star schema.  The data has gotten big enough that they need to start using big data tools like Spark.

2. State and justify the database schema design and ETL pipeline.
The database design is a star schema which is pretty common for extracting business insights.  The ETL pipeline loads log and song data from S3, then creates tables in Spark, and saves it back to S3 as parquet tables.  The data is partitioned according to things like year and month, which make sense in terms of the queries that will be run.


## Loading/writing times

### Writing parquet to S3 with m5.xlarge 3-machine cluster
reading song data...
('took', 134, 'seconds')

writing songs table...
('took', 1366, 's to write songs table')

writing artists table...
('took', 63, 's to write artists table')

reading logs data...
('took', 1, 'seconds')

writing users table...
('took', 8, 's to write users table')

writing time table...
('took', 26, 's to write time table')

reading songs parquet tables...
('took', 945, 's to read')

reading artists parquet tables...
('took', 1, 's to read')

writing songplays table...
('took', 307, 's to write songplays table')

**Total time:** 2851s or 47.5 minutes

### Writing parquet to HDFS with m5.xlarge 3-machine cluster

reading song data...
('took', 138, 'seconds')

writing songs table...
('took', 94, 's to write songs table')

writing artists table...
('took', 47, 's to write artists table')

reading logs data...
('took', 1, 'seconds')

writing users table...
('took', 1, 's to write users table')

writing time table...
('took', 3, 's to write time table')

reading songs parquet tables...
('took', 15, 's to read')

reading artists parquet tables...
('took', 0, 's to read')

joining tables...
('took', 0, 's to join')

writing songplays table...
('took', 19, 's to write songplays table')

#### Copying HDFS to S3

166s

**Total time:** 484s or 8 minutes

**Speedup factor vs S3 direct read/write:** about 6x

### Writing parquet to S3 with m5.2xlarge 5-machine cluster:
reading song data...
('took', 76, 'seconds')

writing songs table...
('took', 394, 's to write songs table')

writing artists table...
('took', 18, 's to write artists table')

reading logs data...
('took', 1, 'seconds')

writing users table...
('took', 3, 's to write users table')

writing time table...
('took', 8, 's to write time table')

reading songs parquet tables...

('took', 936, 's to read')

reading artists parquet tables...
('took', 1, 's to read')

writing songplays table...
('took', 81, 's to write songplays table')

**Total time:** 1518s or 25 minutes

### Writing parquet to HDFS with m5.2xlarge 5-machine cluster:

reading song data...
('took', 74, 'seconds')

writing songs table...
('took', 34, 's to write songs table')

writing artists table...
('took', 11, 's to write artists table')

reading logs data...
('took', 1, 'seconds')

writing users table...
('took', 1, 's to write users table')

writing time table...
('took', 2, 's to write time table')

reading songs parquet tables...
('took', 7, 's to read')

reading artists parquet tables...
('took', 0, 's to read')

joining tables...
('took', 0, 's to join')

writing songplays table...
('took', 8, 's to write songplays table')

#### Copying HDFS to S3

132s

**Total time:** 270s or 4.5 minutes

**Speedup factor vs S3 direct read/write:** about 5.5x
