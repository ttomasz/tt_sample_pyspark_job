import sys
import os
from typing import Optional, List
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, TimestampType, StringType, StructField, LongType


APP_NAME = "Aggregate GitHub Events"

# define schema with fields that we will need
# defining schema makes the process much faster as spark doesn't need to discover it
GH_SCHEMA = StructType([
    StructField('type', StringType(), False),
    StructField('created_at', TimestampType(), False),
    StructField('actor', StructType([
        StructField('id', LongType(), False),
        StructField('login', StringType(), False)
    ]), False),
    StructField('repo', StructType([
        StructField('id', LongType(), False),
        StructField('name', StringType(), False)
    ]), False),
    StructField('payload', StructType([StructField('action', StringType(), True)]), True)
])


def spark_session() -> SparkSession:
    """Method returns instance of Spark Session."""

    return SparkSession \
            .builder \
            .appName(APP_NAME) \
            .getOrCreate()


def read_data(spark: SparkSession, file_path: str, schema: Optional[StructType]) -> DataFrame:
    """Method reads (compressed) json files, projects columns, and returns repartinioned and cached DataFrame."""

    # read json files and select fields that we neeed
    # we will also repartition dataframe by date since later on we will be grouping on that field
    # partitioning by date should help keep data local on workers
    # we will also cache on this step since we are going to use the dataframe a few more times
    df = spark.read.json(file_path, schema=schema)
    df = df.select(
        df['type'].alias('event_type'), 
        df['created_at'].cast('date').alias('date'), 
        df['actor.id'].alias('user_id'), 
        df['actor.login'].alias('user_name'), 
        df['repo.id'].alias('repo_id'), 
        df['repo.name'].alias('repo_name'),
        df['payload.action'].alias('payload_action')
    ).repartition(30, 'date').cache()
    
    return df


def prepare_df(df: DataFrame) -> DataFrame:
    """Method prepares DataFrame with events that we will be analyzing: Forks created, Repositories starred, Issues opened, Pull Request opened."""

    # since user can theoretically star and unstar repos many times a day we will have to decide on the final state for the day i.e. did he/she star a repo or not
    # in the current version github events api only returns 'started' action 
    # we will assume that if a user has multiple such events for the same repo on the same day they starred the repo then unstarred it and starred it again
    # we will drop these duplicated events so in the final statistics it will be counted as one event (on a given date user x starred repo y)
    df_starred = df.filter((df['event_type'] == 'WatchEvent') & (df['payload_action'] == 'started')) \
                   .drop_duplicates(['date', 'user_id', 'repo_id'])
    
    df_other_events = df.filter(
        (df['event_type'] == 'ForkEvent') | \
        ((df['event_type'] == 'IssuesEvent') & (df['payload_action'] == 'opened')) | \
        ((df['event_type'] == 'PullRequestEvent') & (df['payload_action'] == 'opened'))
    )

    return df_starred.unionAll(df_other_events)


def aggregate_user_stats(df: DataFrame) -> DataFrame:
    """Method returns DataFrame with aggregated user statistics.
        Attributes:
         - date
         - user 
        Metrics: 
        - number of starred repos, 
        - issues creted, 
        - pull requests created by user on a given day."""

    df_user_stats = df \
        .select(
            'date', 
            'user_id', 
            'user_name', 
            (df['event_type'] == 'WatchEvent').cast('integer').alias('starred'),  # bool to int conversion, 1=true, 0=false
            (df['event_type'] == 'IssuesEvent').cast('integer').alias('created_issue'), 
            (df['event_type'] == 'PullRequestEvent').cast('integer').alias('created_pr')
        ).groupby(
            'date', 'user_id', 'user_name'
        ).agg(
            F.sum('starred').alias('starred_repos'), 
            F.sum('created_issue').alias('issues_created'), 
            F.sum('created_pr').alias('prs_created')
        )
    
    return df_user_stats


def aggregate_repo_stats(df: DataFrame) -> DataFrame:
    """Method returns DataFrame with aggregated repository statistics.
        Attributes:
         - date
         - repo
        Metrics:
         - number of unique users who starred repo, 
         - number of unique users who forked repo, 
         - number of issues creted, 
         - number of pull requests created."""

    df_repo_stats = df \
        .select(
            'date',
            'repo_id',
            'repo_name',
            F.when(df['event_type'] == 'WatchEvent', df['user_id']).alias('user_who_starred_repo'),
            F.when(df['event_type'] == 'ForkEvent', df['user_id']).alias('user_who_forked_repo'),
            (df['event_type'] == 'IssuesEvent').cast('integer').alias('created_issue'),
            (df['event_type'] == 'PullRequestEvent').cast('integer').alias('created_pr')
        ).groupby(
            'date', 'repo_id', 'repo_name'
        ).agg(
            F.countDistinct('user_who_starred_repo').alias('distinct_users_who_starred_repo'),
            F.countDistinct('user_who_forked_repo').alias('distinct_users_who_forked_repo'),
            F.sum('created_issue').alias('issues_created'), 
            F.sum('created_pr').alias('prs_created')
        )
    
    return df_repo_stats


def write_parquet(df: DataFrame, path: str, mode: str = 'overwrite', partition_by: Optional[List[str]] = None) -> None:
    """Method reduces number of partition then saves the DataFrame as Parquet files with Snappy compression. 
    You can choose what to do when files already exist by setting mode (overwrite, append, error).
    You can also specify list of columns names to partition by."""

    # when partitions are too small it can have negative performance effect
    # too few partitions can bottleneck parallel writing
    # since the data in this task is not huge in size we'll coalesce it into a single partition
    df.coalesce(1).write.parquet(path=path, mode=mode, partitionBy=partition_by, compression='snappy')
    # we could also achieve better compression rates by sorting the DF by user_id or repo_id


if __name__ == "__main__":

    # file path(s) will be provided as a parameter when submitting the job
    # path supports wildcards
    input_fp = sys.argv[1]
    # output directory will also be provided as a parameter
    # output file names are set to specific values at the moment but if it was needed script can be changed to allow file names as parameters too
    output_dir = sys.argv[2]

    spark = spark_session()

    # read
    df = read_data(spark, input_fp, GH_SCHEMA)
    df = prepare_df(df)

    # aggregate
    df_user_stats = aggregate_user_stats(df)
    df_repo_stats = aggregate_repo_stats(df)
    
    # save
    user_stats_output_path = os.path.join(output_dir, 'user_stats.parquet')
    write_parquet(df_user_stats, user_stats_output_path)

    repo_stats_output_path = os.path.join(output_dir, 'repo_stats.parquet')
    write_parquet(df_repo_stats, repo_stats_output_path)
