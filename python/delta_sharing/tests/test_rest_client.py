#
# Copyright (C) 2021 The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import pytest

from delta_sharing.protocol import (
    AddFile,
    Format,
    Metadata,
    Protocol,
    Schema,
    Share,
    Table,
)
from delta_sharing.rest_client import DataSharingRestClient
from delta_sharing.tests.conftest import ENABLE_INTEGRATION, SKIP_MESSAGE


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_read_endpoint(rest_client: DataSharingRestClient):
    assert not rest_client._profile.endpoint.endswith("/")


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_shares(rest_client: DataSharingRestClient):
    response = rest_client.list_shares()
    assert response.shares == [
        Share(name="share1"),
        Share(name="share2"),
        Share(name="share3"),
        Share(name="share4"),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_schemas(rest_client: DataSharingRestClient):
    response = rest_client.list_schemas(Share(name="share1"))
    assert response.schemas == [Schema(name="default", share="share1")]

    response = rest_client.list_schemas(Share(name="share2"))
    assert response.schemas == [Schema(name="default", share="share2")]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_tables(rest_client: DataSharingRestClient):
    response = rest_client.list_tables(Schema(name="default", share="share1"))
    assert response.tables == [
        Table(name="table1", share="share1", schema="default"),
        Table(name="table3", share="share1", schema="default"),
        Table(name="table7", share="share1", schema="default"),
    ]

    response = rest_client.list_tables(Schema(name="default", share="share2"))
    assert response.tables == [Table(name="table2", share="share2", schema="default")]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_query_table_metadata_non_partitioned(rest_client: DataSharingRestClient):
    response = rest_client.query_table_metadata(
        Table(name="table1", share="share1", schema="default")
    )
    assert response.protocol == Protocol(min_reader_version=1)
    assert response.metadata == Metadata(
        id="ed96aa41-1d81-4b7f-8fb5-846878b4b0cf",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=[],
    )


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_query_table_metadata_partitioned(rest_client: DataSharingRestClient):
    response = rest_client.query_table_metadata(
        Table(name="table2", share="share2", schema="default")
    )
    assert response.protocol == Protocol(min_reader_version=1)
    assert response.metadata == Metadata(
        id="f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=["date"],
    )


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_query_table_metadata_partitioned_different_schemas(rest_client: DataSharingRestClient):
    response = rest_client.query_table_metadata(
        Table(name="table3", share="share1", schema="default")
    )
    assert response.protocol == Protocol(min_reader_version=1)
    assert response.metadata == Metadata(
        id="7ba6d727-a578-4234-a138-953f790b427c",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}},'
            '{"name":"type","type":"string","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=["date"],
    )


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_files_in_table_non_partitioned(rest_client: DataSharingRestClient):
    response = rest_client.list_files_in_table(
        Table(name="table1", share="share1", schema="default"),
        predicateHints=["date = '2021-01-31'"],
    )
    assert response.protocol == Protocol(min_reader_version=1)
    assert response.metadata == Metadata(
        id="ed96aa41-1d81-4b7f-8fb5-846878b4b0cf",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=[],
    )
    assert response.add_files == [
        AddFile(
            url=response.add_files[0].url,
            id="061cb3683a467066995f8cdaabd8667d",
            partition_values={},
            size=781,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},'
                r'"maxValues":{"eventTime":"2021-04-28T06:32:22.421Z","date":"2021-04-28"},'
                r'"nullCount":{"eventTime":0,"date":0}}'
            ),
        ),
        AddFile(
            url=response.add_files[1].url,
            id="e268cbf70dbaa6143e7e9fa3e2d3b00e",
            partition_values={},
            size=781,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T06:32:02.070Z","date":"2021-04-28"},'
                r'"maxValues":{"eventTime":"2021-04-28T06:32:02.070Z","date":"2021-04-28"},'
                r'"nullCount":{"eventTime":0,"date":0}}'
            ),
        ),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_files_in_table_partitioned(rest_client: DataSharingRestClient):
    response = rest_client.list_files_in_table(
        Table(name="table2", share="share2", schema="default"),
        predicateHints=["date = '2021-01-31'"],
        limitHint=123,
    )
    assert response.protocol == Protocol(min_reader_version=1)
    assert response.metadata == Metadata(
        id="f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=["date"],
    )
    assert response.add_files == [
        AddFile(
            url=response.add_files[0].url,
            id="9f1a49539c5cffe1ea7f9e055d5c003c",
            partition_values={"date": "2021-04-28"},
            size=573,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T23:33:57.955Z"},'
                r'"maxValues":{"eventTime":"2021-04-28T23:33:57.955Z"},'
                r'"nullCount":{"eventTime":0}}'
            ),
        ),
        AddFile(
            url=response.add_files[1].url,
            id="cd2209b32f5ed5305922dd50f5908a75",
            partition_values={"date": "2021-04-28"},
            size=573,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T23:33:48.719Z"},'
                r'"maxValues":{"eventTime":"2021-04-28T23:33:48.719Z"},'
                r'"nullCount":{"eventTime":0}}'
            ),
        ),
    ]


@pytest.mark.skipif(not ENABLE_INTEGRATION, reason=SKIP_MESSAGE)
def test_list_files_in_table_partitioned_different_schemas(rest_client: DataSharingRestClient):
    response = rest_client.list_files_in_table(
        Table(name="table3", share="share1", schema="default")
    )
    assert response.protocol == Protocol(min_reader_version=1)
    assert response.metadata == Metadata(
        id="7ba6d727-a578-4234-a138-953f790b427c",
        format=Format(provider="parquet", options={}),
        schema_string=(
            '{"type":"struct","fields":['
            '{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},'
            '{"name":"date","type":"date","nullable":true,"metadata":{}},'
            '{"name":"type","type":"string","nullable":true,"metadata":{}}'
            "]}"
        ),
        partition_columns=["date"],
    )
    assert response.add_files == [
        AddFile(
            url=response.add_files[0].url,
            id="db213271abffec6fd6c7fc2aad9d4b3f",
            partition_values={"date": "2021-04-28"},
            size=778,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T23:36:51.945Z","type":"bar"},'
                r'"maxValues":{"eventTime":"2021-04-28T23:36:51.945Z","type":"bar"},'
                r'"nullCount":{"eventTime":0,"type":0}}'
            ),
        ),
        AddFile(
            url=response.add_files[1].url,
            id="f1f8be229d8b18eb6d6a34255f2d7089",
            partition_values={"date": "2021-04-28"},
            size=778,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T23:36:47.599Z","type":"foo"},'
                r'"maxValues":{"eventTime":"2021-04-28T23:36:47.599Z","type":"foo"},'
                r'"nullCount":{"eventTime":0,"type":0}}'
            ),
        ),
        AddFile(
            url=response.add_files[2].url,
            id="a892a55d770ee70b34ffb2ebf7dc2fd0",
            partition_values={"date": "2021-04-28"},
            size=573,
            stats=(
                r'{"numRecords":1,'
                r'"minValues":{"eventTime":"2021-04-28T23:35:53.156Z"},'
                r'"maxValues":{"eventTime":"2021-04-28T23:35:53.156Z"},'
                r'"nullCount":{"eventTime":0}}'
            ),
        ),
    ]
