## Sparkify Data Lake Project:
This is Project 4 in the Udacity Data Engineer Nanodegree.
It  creates a data lake in AWS for the ficticious *Sparkify* online music streaming company.

### Data Sources
Data comes from two sources:
1) Song data is available from the [million song
data set](http://millionsongdataset.com/)
2) Event data is available from the [eventsim
github](https://github.com/Interana/eventsim). Eventsim is a data simulator for
user actions on a fictitious song streaming webpage.  Users can register for
the platform, listen to songs, change their memberships from free to paid and
vice versa and more.

Both data sets are stored together on [Udacity's own S3
bucket](s3://udacity-dend/) which is
publically read excessible.

### Data Destination
The `etl.py` script migrates the data from Udacity's S3 bucket to a datalake on
your own private S3 bucket on AWS.

The *transformation* part of the etl comes from transforming the raw log data
into a star schema of 5 tables which are more useful for OLAP analytics.  This
allows simpler SQL queries to answer questions like, which are the most popular
songs by year, when are our users online, and what type of music do our
listeners like best.


### Run Instructions
First create a iam user on AWS and obtain a programatic key, secret pair.
Maker sure you have S3 read and write access.

Next, Create a personal S3 bucket to hold the star schema data.

Then spin up an EMR cluster to run the `etl.py` script.  This will transfer
data to your datalake.
