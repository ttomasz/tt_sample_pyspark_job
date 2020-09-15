# Sample Spark job in python

This repository contains code for a sample Spark job written in Python. Code processes GitHub events data and aggregates some information about them.

More about source data at: http://www.gharchive.org/

Structure of output data in _Results_ section.

## TL;DR

Run these commands:

(change directory 'F:/spark-task' for the directory where you cloned this repo on your machine)
```
$ docker run -d -p 8888:8888 -p 4040:4040 -p 4041:4041 -p 4042:4042 --name sparky -v F:/spark-task:/home/jovyan/work jupyter/all-spark-notebook:13b866ff67b9
$ cd /directory where you cloned this repo/
$ bash ./download_data.sh
$ docker exec -it sparky python3 ./work/test_aggregate.py
$ docker exec -i sparky /usr/local/spark/bin/spark-submit --master local[3] /home/jovyan/work/aggregate.py file:///home/jovyan/work/raw_data/*.json.gz file:///home/jovyan/work/output_data/
```

## Environment

I used Jupyter's docker images to set up Spark. Their images create singe machine by default so no dedicated workers but it should be enough for this task.

You can run the container using command (change directory 'F:/spark-task' for the directory where you cloned this repo on your machine):
```
docker run -d -p 8888:8888 -p 4040:4040 -p 4041:4041 -p 4042:4042 --name sparky -v F:/spark-task:/home/jovyan/work jupyter/all-spark-notebook:13b866ff67b9
```
You can also choose another name for the container by changing --name parameter (if you do remember to use your name in the rest of the commands).

Container maps internal directory /home/jovyan/work to the directory on the host machine so Spark can access the files. In a regular production setup files would probably be stored in HDFS or S3.

Ports 8888, 4040-2 are mapped to the same ports on your machine. Port 8888 can be used to access jupyter notebook. Ports 4040 to 4042 are used by Spark Web UI which is created when Spark Context is created.

If you want to access jupyter you can run:
```
docker logs --tail 3 sparky
```
just after launching container to get the url with the token required by jupyter on launch.

When a job is running you can track stats and see details like DAG using Spark Web UI, server for that is only online when Spark Context is active so after job is run it might shut down. These contexts/servers take ports starting at 4040 and incrementing when given port is already in use that's why the container is mapping a few ports just in case you need too access ui for multiple contexts.

URL for web UI when it's active:
```
http://localhost:4040/jobs/
```
(or another port as described above)

## Data

Data can be downloaded using wget command.

You can use script in repo: _download_data.sh_ to download files for the entire month (2018-01). Files will be saved in _raw_data_ directory.

Example command:
```
$ cd /directory where you cloned this repo/
$ bash ./download_data.sh
```

## Code

Code is in Python. I opted to use DataFrame API since it's more similar to technologies I am familiar with.

Code could be rewritten using map/reduce concepts eg:

Iterate over records in files and map them with a function emitting key consisting of date, user id, user name, event type and value 1. There would be a few of those functions mapping for different aggregates.

Then use reducer function to sum those values.

Then pivot the data into a table and export it to an optimized format such as orc or parquet or avro which should be much faster to access and much more compact than json or csv.

## Unit tests

Some basic tests are present. You can launch them using this command:
```
docker exec -it sparky python3 ./work/test_aggregate.py
```

These tests could use improvement as they don't cover many scenarios. There is only single row of each type in the test data.

## Submitting jobs

To submit job to Spark in container you can use command like:
```
docker exec -i sparky /usr/local/spark/bin/spark-submit --master local[3] /home/jovyan/work/aggregate.py file:///home/jovyan/work/raw_data/2018-01-01-0.json.gz file:///home/jovyan/work/output_data/
```

Since this docker image does not provide a cluster we will use "local" as master. Specify number of available cores with the number in square brackets.

On a production-grade cluster we would specify more parameters that could eg limit resources for the job, pick resource manager and master location etc.

Then you provide path to the entry point/class ie script to run. (remember we defined docker volume mapping for /home/jovyan/work)

Then you provide file path for file(s) with the data. You can use wildcards in the filepath.

Last parameter is directory where we save the results.

Filepaths specify protocol for access since depending on environment configuration it could be eg S3 or HDFS storage. In this case we are just using file protocol.

__To process all the downloaded data run:__
```
docker exec -i sparky /usr/local/spark/bin/spark-submit --master local[3] /home/jovyan/work/aggregate.py file:///home/jovyan/work/raw_data/*.json.gz file:///home/jovyan/work/output_data/
```

## Results

Results are saved to two partitioned Parquet files.

Parquet format provides very good read performance and keeps the size of the files small.

Below there are some example rows from the final DataFrame to show the structure.

Data about users:

|      date| user_id|       user_name|starred_repos|issues_created|prs_created|
|----------|--------|----------------|-------------|--------------|-----------|
|2018-01-01| 1223268|       yoheimuta|            0|             0|          0|
|2018-01-01|12697458|        s3421153|            0|             0|          0|
|2018-01-01|33162051|       Whurrhurr|            0|             0|          0|
|2018-01-01|   46317|            cori|            0|             0|          0|
|2018-01-01|18902105|azstaylorswift13|            0|             0|          0|
|2018-01-01| 7039523|        arun1595|            0|             0|          0|
|2018-01-01| 5892997|          dwhieb|            0|             1|          0|
|2018-01-01| 5621298|            at15|            0|             1|          0|
|2018-01-01|17896438|            jd20|            0|             2|          0|
|2018-01-01|20820418|           meass|            0|             1|          0|
|2018-01-01|   41616|        zendyani|            1|             0|          0|
|2018-01-01|  532195|      MatheusMK3|            1|             0|          0|
|2018-01-01|  734056|         chao787|            2|             0|          0|
|2018-01-01| 1160017|         xcopier|            1|             0|          0|
|2018-01-01| 1884170|       willisweb|            1|             0|          0|
|2018-01-01| 1887768|         fazlurr|            1|             0|          0|
|2018-01-01| 2133825|         jokbull|            3|             0|          0|
|2018-01-01| 2605791| Lewiscowles1986|            1|             0|          0|
|2018-01-01| 3012882|       wangkezun|            2|             0|          0|
|2018-01-01| 4725056|   AmrishJhaveri|           17|             0|          0|

Data about repositories:

|      date|  repo_id|           repo_name|distinct_users_who_starred_repo|distinct_users_who_forked_repo|issues_created|prs_created|
|----------|---------|--------------------|-------------------------------|------------------------------|--------------|-----------|
|2018-01-01|111300000|shizhouwang/linux...|                              0|                             0|             1|          0|
|2018-01-01|115892677|        kokSS5/Task1|                              1|                             0|             0|          0|
|2018-01-01|  7131933|wet-boew/web-repo...|                              1|                             2|             0|          0|
|2018-01-01| 75664242|  topjohnwu/MagiskSU|                              1|                             0|             0|          0|
|2018-01-01| 49084741|j-delaney/easy-ap...|                              0|                             1|             0|          0|
|2018-01-01| 19905299|  angelXwind/AppSync|                              1|                             0|             0|          0|
|2018-01-01| 75104123|   prettier/prettier|                              3|                             1|             0|          0|
|2018-01-01|  4832534|weiran/AFXAuthClient|                              0|                             1|             0|          0|
|2018-01-01| 94618560|Kyubyong/transformer|                              1|                             0|             0|          0|
|2018-01-01|  8418302|     fronteed/icheck|                              1|                             0|             0|          0|
|2018-01-01| 96801852|akashsara/whatsap...|                              0|                             1|             0|          0|
|2018-01-01| 83234338|  JEMeyer/Neural.NET|                              1|                             0|             0|          0|
|2018-01-01|  8121685|      mybatis/parent|                              1|                             0|             0|          0|
|2018-01-01|  7658948|   wet-boew/bookmark|                              1|                             1|             0|          0|
|2018-01-01| 94150625|secretyouth/react...|                              1|                             0|             0|          0|
|2018-01-01| 55711556|zzzprojects/Dappe...|                              1|                             0|             0|          0|
|2018-01-01| 62728051| minishift/minishift|                              1|                             0|             0|          0|
|2018-01-01| 57180853|    Tencent/vConsole|                              1|                             0|             0|          0|
|2018-01-01| 42183658|stereolabs/zed-ro...|                              0|                             1|             0|          0|
|2018-01-01|113005203|samanyougarg/hanuman|                              0|                             2|             0|          0|
