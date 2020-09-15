import unittest
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
import pandas as pd
import datetime
import shutil

import aggregate


TEST_APP_NAME = 'TEST ' + aggregate.APP_NAME
TEST_DATA_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data.jsonl')


class Test_Spark(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder \
            .master('local') \
            .appName(TEST_APP_NAME) \
            .config('spark.driver.host", "127.0.0.1') \
            .getOrCreate()
        self.user_data_path = './tmp_user_data'
        self.repo_data_path = './tmp_repo_data'


    def tearDown(self):
        if os.path.exists(self.user_data_path):
            shutil.rmtree(self.user_data_path)
        if os.path.exists(self.repo_data_path):
            shutil.rmtree(self.repo_data_path)


    def test_loading_data(self):
        ts = datetime.datetime(2018, 1, 1, 0, 0, 1)
        correct_data = self.spark.createDataFrame([
            {"type": "ForkEvent", "created_at": ts, "actor": {"id": 1, "login": "login_1"}, "repo": {"id": 1, "name": "repo_1"}},
            {"type": "IssuesEvent", "created_at": ts, "actor": {"id": 1, "login": "login_1"}, "repo": {"id": 1, "name": "repo_1"}, "payload": {"action": "opened"}},
            {"type": "PullRequestEvent", "created_at": ts, "actor": {"id": 1, "login": "login_1"}, "repo": {"id": 1, "name": "repo_1"}, "payload": {"action": "opened"}},
            {"type": "WatchEvent", "created_at": ts, "actor": {"id": 1, "login": "login_1"}, "repo": {"id": 1, "name": "repo_1"}, "payload": {"action": "started"}},
            {"type": "other_event", "created_at": ts, "actor": {"id": 1, "login": "login_1"}, "repo": {"id": 1, "name": "repo_1"}, "payload": {"test": "test"}, "id": 123},
        ], aggregate.GH_SCHEMA)
        correct_data = correct_data.select(
            correct_data['type'].alias('event_type'), 
            correct_data['created_at'].cast('date').alias('date'), 
            correct_data['actor.id'].alias('user_id'), 
            correct_data['actor.login'].alias('user_name'), 
            correct_data['repo.id'].alias('repo_id'), 
            correct_data['repo.name'].alias('repo_name'),
            correct_data['payload.action'].alias('payload_action')
        )

        data_from_json = aggregate.read_data(self.spark, TEST_DATA_FILE_PATH, aggregate.GH_SCHEMA)
        col_list = ['event_type', 'date', 'user_id', 'user_name', 'repo_id', 'repo_name', 'payload_action']

        pd.testing.assert_frame_equal(sorted_df(correct_data.toPandas(), col_list), sorted_df(data_from_json.toPandas(), col_list), check_like=True)


    def test_preparing_df(self):
        ts = datetime.datetime(2018, 1, 1, 0, 0, 1)
        correct_data = self.spark.createDataFrame([
            {"type": "ForkEvent", "created_at": ts, "actor": {"id": 1, "login": "login_1"}, "repo": {"id": 1, "name": "repo_1"}},
            {"type": "IssuesEvent", "created_at": ts, "actor": {"id": 1, "login": "login_1"}, "repo": {"id": 1, "name": "repo_1"}, "payload": {"action": "opened"}},
            {"type": "PullRequestEvent", "created_at": ts, "actor": {"id": 1, "login": "login_1"}, "repo": {"id": 1, "name": "repo_1"}, "payload": {"action": "opened"}},
            {"type": "WatchEvent", "created_at": ts, "actor": {"id": 1, "login": "login_1"}, "repo": {"id": 1, "name": "repo_1"}, "payload": {"action": "started"}},
        ], aggregate.GH_SCHEMA)
        correct_data = correct_data.select(
            correct_data['type'].alias('event_type'), 
            correct_data['created_at'].cast('date').alias('date'), 
            correct_data['actor.id'].alias('user_id'), 
            correct_data['actor.login'].alias('user_name'), 
            correct_data['repo.id'].alias('repo_id'), 
            correct_data['repo.name'].alias('repo_name'),
            correct_data['payload.action'].alias('payload_action')
        )

        data_from_json = aggregate.read_data(self.spark, TEST_DATA_FILE_PATH, aggregate.GH_SCHEMA)
        prepared_df = aggregate.prepare_df(data_from_json)
        col_list = ['event_type', 'date', 'user_id', 'user_name', 'repo_id', 'repo_name', 'payload_action']

        pd.testing.assert_frame_equal(sorted_df(correct_data.toPandas(), col_list), sorted_df(prepared_df.toPandas(), col_list), check_like=True)
    

    def test_aggregating_user_data(self):
        dd = datetime.datetime(2018, 1, 1, 0, 0, 1).date()
        correct_data = self.spark.createDataFrame([
            {'date': dd, 'user_id': 1, 'user_name': 'login_1', 'starred_repos': 1, 'issues_created': 1, 'prs_created': 1}
        ])

        data_from_json = aggregate.read_data(self.spark, TEST_DATA_FILE_PATH, aggregate.GH_SCHEMA)
        prepared_df = aggregate.prepare_df(data_from_json)
        aggregated_user_data = aggregate.aggregate_user_stats(prepared_df)
        col_list = ['date', 'user_id', 'user_name', 'starred_repos', 'issues_created', 'prs_created']

        pd.testing.assert_frame_equal(sorted_df(correct_data.toPandas(), col_list), sorted_df(aggregated_user_data.toPandas(), col_list), check_like=True)
    

    def test_aggregating_repo_data(self):
        dd = datetime.datetime(2018, 1, 1, 0, 0, 1).date()
        correct_data = self.spark.createDataFrame([
            {'date': dd, 'repo_id': 1, 'repo_name': 'repo_1', 'distinct_users_who_starred_repo': 1, 
            'distinct_users_who_forked_repo': 1, 'issues_created': 1, 'prs_created': 1}
        ])

        data_from_json = aggregate.read_data(self.spark, TEST_DATA_FILE_PATH, aggregate.GH_SCHEMA)
        prepared_df = aggregate.prepare_df(data_from_json)
        aggregated_repo_data = aggregate.aggregate_repo_stats(prepared_df)
        col_list = ['date', 'repo_id', 'repo_name', 'distinct_users_who_starred_repo', 'distinct_users_who_forked_repo', 'issues_created', 'prs_created']

        pd.testing.assert_frame_equal(sorted_df(correct_data.toPandas(), col_list), sorted_df(aggregated_repo_data.toPandas(), col_list), check_like=True)


    def test_writing_user_stats(self):
        dd = datetime.datetime(2018, 1, 1, 0, 0, 1).date()
        correct_data = self.spark.createDataFrame([
            {'date': dd, 'user_id': 1, 'user_name': 'login_1', 'starred_repos': 1, 'issues_created': 1, 'prs_created': 1}
        ])

        data_from_json = aggregate.read_data(self.spark, TEST_DATA_FILE_PATH, aggregate.GH_SCHEMA)
        prepared_df = aggregate.prepare_df(data_from_json)
        aggregated_user_data = aggregate.aggregate_user_stats(prepared_df)
        aggregate.write_parquet(aggregated_user_data, self.user_data_path)
        df_from_file = self.spark.read.parquet(self.user_data_path)
        col_list = ['date', 'user_id', 'user_name', 'starred_repos', 'issues_created', 'prs_created']

        pd.testing.assert_frame_equal(sorted_df(correct_data.toPandas(), col_list), sorted_df(df_from_file.toPandas(), col_list), check_like=True)


    def test_writing_repo_stats(self):
        dd = datetime.datetime(2018, 1, 1, 0, 0, 1).date()
        correct_data = self.spark.createDataFrame([
            {'date': dd, 'repo_id': 1, 'repo_name': 'repo_1', 'distinct_users_who_starred_repo': 1, 
            'distinct_users_who_forked_repo': 1, 'issues_created': 1, 'prs_created': 1}
        ])

        data_from_json = aggregate.read_data(self.spark, TEST_DATA_FILE_PATH, aggregate.GH_SCHEMA)
        prepared_df = aggregate.prepare_df(data_from_json)
        aggregated_repo_data = aggregate.aggregate_repo_stats(prepared_df)
        aggregate.write_parquet(aggregated_repo_data, self.repo_data_path)
        df_from_file = self.spark.read.parquet(self.repo_data_path)
        col_list = ['date', 'repo_id', 'repo_name', 'distinct_users_who_starred_repo', 'distinct_users_who_forked_repo', 'issues_created', 'prs_created']

        pd.testing.assert_frame_equal(sorted_df(correct_data.toPandas(), col_list), sorted_df(df_from_file.toPandas(), col_list), check_like=True)


def sorted_df(data_frame, columns_list):
    """Helper method that sorts rows in PANDAS dataframe so we can compare it. 
    Make sure that you pass pandas dataframe not spark dataframe."""

    return data_frame.sort_values(columns_list).reset_index(drop=True)


if __name__ == '__main__':
    unittest.main()
