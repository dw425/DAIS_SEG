"""Tests for parsers: format detection, NiFi XML/JSON, SSIS, Airflow, SQL, etc."""

import json

import pytest

from app.engines.parsers.dispatcher import parse_flow

# ── NiFi XML ──

NIFI_TEMPLATE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<template>
  <snippet>
    <processors>
      <name>Fetch Data</name>
      <type>org.apache.nifi.processors.standard.GetFile</type>
      <id>proc-1</id>
      <state>RUNNING</state>
      <config>
        <schedulingPeriod>5 sec</schedulingPeriod>
        <schedulingStrategy>TIMER_DRIVEN</schedulingStrategy>
        <properties>
          <entry><key>Input Directory</key><value>/data/input</value></entry>
        </properties>
      </config>
    </processors>
    <processors>
      <name>Write Output</name>
      <type>org.apache.nifi.processors.standard.PutFile</type>
      <id>proc-2</id>
      <state>RUNNING</state>
      <config>
        <properties>
          <entry><key>Directory</key><value>/data/output</value></entry>
        </properties>
      </config>
    </processors>
    <connections>
      <source><id>proc-1</id></source>
      <destination><id>proc-2</id></destination>
      <selectedRelationships>success</selectedRelationships>
    </connections>
    <controllerServices>
      <controllerService>
        <name>DBCP Pool</name>
        <type>org.apache.nifi.dbcp.DBCPConnectionPool</type>
        <properties>
          <entry><key>Database Connection URL</key><value>jdbc:mysql://localhost:3306/db</value></entry>
        </properties>
      </controllerService>
    </controllerServices>
  </snippet>
</template>
"""


def test_nifi_xml_parse():
    result = parse_flow(NIFI_TEMPLATE_XML, "test.xml")
    assert result.platform == "nifi"
    assert len(result.processors) == 2
    assert result.processors[0].name == "Fetch Data"
    assert result.processors[0].type == "GetFile"
    assert result.processors[0].platform == "nifi"
    assert result.processors[0].properties.get("Input Directory") == "/data/input"
    assert len(result.connections) == 1
    assert result.connections[0].source_name == "Fetch Data"
    assert result.connections[0].destination_name == "Write Output"
    assert len(result.controller_services) == 1
    assert result.controller_services[0].name == "DBCP Pool"


def test_nifi_xml_process_groups():
    xml = b"""<?xml version="1.0" encoding="UTF-8"?>
    <template><snippet>
      <processGroups>
        <name>ETL Group</name>
        <id>pg-1</id>
        <contents>
          <processors>
            <name>Inner Proc</name>
            <type>org.apache.nifi.processors.standard.ReplaceText</type>
            <id>inner-1</id>
          </processors>
        </contents>
      </processGroups>
    </snippet></template>
    """
    result = parse_flow(xml, "groups.xml")
    assert len(result.process_groups) == 1
    assert result.process_groups[0].name == "ETL Group"
    assert len(result.processors) == 1
    assert result.processors[0].group == "ETL Group"


# ── NiFi JSON ──


def test_nifi_json_parse():
    data = {
        "flowContents": {
            "name": "root",
            "processors": [
                {
                    "identifier": "p1",
                    "name": "Generate",
                    "type": "org.apache.nifi.processors.standard.GenerateFlowFile",
                    "properties": {"File Size": "1 KB"},
                    "scheduledState": "RUNNING",
                },
                {
                    "identifier": "p2",
                    "name": "Log",
                    "type": "org.apache.nifi.processors.standard.LogAttribute",
                    "properties": {},
                    "scheduledState": "RUNNING",
                },
            ],
            "connections": [
                {
                    "source": {"id": "p1", "name": "Generate"},
                    "destination": {"id": "p2", "name": "Log"},
                    "selectedRelationships": ["success"],
                }
            ],
        }
    }
    content = json.dumps(data).encode()
    result = parse_flow(content, "flow.json")
    assert result.platform == "nifi"
    assert len(result.processors) == 2
    assert result.processors[0].name == "Generate"
    assert result.processors[0].type == "GenerateFlowFile"
    assert len(result.connections) == 1


# ── SSIS ──

SSIS_DTSX = b"""<?xml version="1.0"?>
<DTS:Executable xmlns:DTS="www.microsoft.com/SqlServer/Dts"
  DTS:ObjectName="TestPackage" DTS:CreationName="SSIS.Package.3">
  <DTS:Executables>
    <DTS:Executable DTS:ObjectName="Data Flow Task" DTS:CreationName="SSIS.Pipeline.3" DTS:DTSID="dft-1">
    </DTS:Executable>
    <DTS:Executable DTS:ObjectName="Execute SQL"
      DTS:CreationName="Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask"
      DTS:DTSID="sql-1">
    </DTS:Executable>
  </DTS:Executables>
  <DTS:PrecedenceConstraints>
    <DTS:PrecedenceConstraint DTS:From="dft-1" DTS:To="sql-1" DTS:Value="0"/>
  </DTS:PrecedenceConstraints>
</DTS:Executable>
"""


def test_ssis_parse():
    result = parse_flow(SSIS_DTSX, "package.dtsx")
    assert result.platform == "ssis"
    assert len(result.processors) >= 1


# ── Airflow ──

AIRFLOW_DAG = b"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

dag = DAG("my_etl_dag", start_date=datetime(2024, 1, 1))

extract = BashOperator(task_id="extract", bash_command="echo extract", dag=dag)
transform = PythonOperator(task_id="transform", python_callable=lambda: None, dag=dag)
load = BashOperator(task_id="load", bash_command="echo load", dag=dag)

extract >> transform >> load
"""


def test_airflow_parse():
    result = parse_flow(AIRFLOW_DAG, "dag.py")
    assert result.platform == "airflow"
    assert len(result.processors) == 3
    assert any(p.name == "extract" for p in result.processors)
    assert len(result.connections) >= 2


# ── SQL ──

SQL_FILE = b"""
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(200)
);

INSERT INTO customers_staging
SELECT * FROM raw_customers;
"""


def test_sql_parse():
    result = parse_flow(SQL_FILE, "schema.sql")
    assert result.platform == "sql"
    assert len(result.processors) >= 1


# ── dbt manifest ──


def test_dbt_manifest_parse():
    manifest = {
        "metadata": {"dbt_version": "1.5.0"},
        "nodes": {
            "model.my_project.stg_customers": {
                "resource_type": "model",
                "name": "stg_customers",
                "schema": "staging",
                "config": {"materialized": "view"},
                "depends_on": {"nodes": ["source.my_project.raw_customers"]},
            }
        },
        "sources": {
            "source.my_project.raw_customers": {
                "name": "raw_customers",
                "source_name": "raw",
                "loader": "postgres",
            }
        },
    }
    content = json.dumps(manifest).encode()
    result = parse_flow(content, "manifest.json")
    assert result.platform == "dbt"
    assert len(result.processors) >= 2


# ── Format detection ──


def test_format_detection_xml():
    result = parse_flow(NIFI_TEMPLATE_XML, "unknown_file.xml")
    assert result.platform == "nifi"


def test_format_detection_unsupported():
    with pytest.raises(Exception):
        parse_flow(b"random binary data", "file.xyz")
