#!/bin/bash
s3-dist-cp --src hdfs:///dwh/songs --dest s3://udacity-dend-spark-dwh3/songs
s3-dist-cp --src hdfs:///dwh/artists --dest s3://udacity-dend-spark-dwh3/artists
s3-dist-cp --src hdfs:///dwh/users --dest s3://udacity-dend-spark-dwh3/users
s3-dist-cp --src hdfs:///dwh/time --dest s3://udacity-dend-spark-dwh3/time
s3-dist-cp --src hdfs:///dwh/songplays --dest s3://udacity-dend-spark-dwh3/songplays
