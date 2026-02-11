"""Tests for the Universal Source Loader module."""

import json
import pytest

from dais_seg.source_loader.base import (
    InputFormat, ParsedColumn, ParsedForeignKey, ParsedTable, ParsedSchema,
)
from dais_seg.source_loader.detector import FormatDetector
from dais_seg.source_loader.ddl_parser import DDLParser
from dais_seg.source_loader.etl_mapping_parser import ETLMappingParser
from dais_seg.source_loader.schema_definition_parser import SchemaDefinitionParser
from dais_seg.source_loader.default_stats import generate_default_stats
from dais_seg.source_loader.blueprint_assembler import BlueprintAssembler


# ---------------------------------------------------------------------------
# FormatDetector
# ---------------------------------------------------------------------------

class TestFormatDetector:

    def test_detect_sql_extension(self):
        assert FormatDetector.detect("anything", "schema.sql") == InputFormat.DDL

    def test_detect_ddl_extension(self):
        assert FormatDetector.detect("anything", "schema.ddl") == InputFormat.DDL

    def test_detect_csv_extension(self):
        assert FormatDetector.detect("anything", "mapping.csv") == InputFormat.ETL_MAPPING

    def test_detect_xlsx_extension(self):
        assert FormatDetector.detect("anything", "mapping.xlsx") == InputFormat.ETL_MAPPING

    def test_detect_json_extension(self):
        assert FormatDetector.detect("anything", "schema.json") == InputFormat.SCHEMA_JSON

    def test_detect_yaml_extension(self):
        assert FormatDetector.detect("anything", "schema.yaml") == InputFormat.SCHEMA_YAML
        assert FormatDetector.detect("anything", "schema.yml") == InputFormat.SCHEMA_YAML

    def test_detect_env_extension(self):
        assert FormatDetector.detect("anything", "creds.env") == InputFormat.ENV_FILE

    def test_detect_ddl_from_content(self):
        assert FormatDetector.detect("CREATE TABLE foo (id INT);") == InputFormat.DDL

    def test_detect_json_from_content(self):
        assert FormatDetector.detect('{"tables": []}') == InputFormat.SCHEMA_JSON

    def test_detect_yaml_from_content(self):
        assert FormatDetector.detect("tables:\n  - name: foo") == InputFormat.SCHEMA_YAML

    def test_detect_etl_from_content(self):
        assert FormatDetector.detect("source_table,source_column,target_table,target_column\nfoo,id,bar,id") == InputFormat.ETL_MAPPING

    def test_detect_env_from_content(self):
        assert FormatDetector.detect("SOURCE_TYPE=oracle\nSOURCE_HOST=h") == InputFormat.ENV_FILE

    def test_detect_raw_text_with_type_keywords(self):
        assert FormatDetector.detect("id INT, name VARCHAR(100)") == InputFormat.RAW_TEXT

    def test_detect_unknown(self):
        assert FormatDetector.detect("random text with nothing useful") == InputFormat.UNKNOWN

    def test_detect_empty(self):
        assert FormatDetector.detect("") == InputFormat.UNKNOWN

    def test_extension_takes_priority(self):
        # Even if content looks like DDL, .csv extension wins
        assert FormatDetector.detect("CREATE TABLE foo (id INT);", "data.csv") == InputFormat.ETL_MAPPING


# ---------------------------------------------------------------------------
# DDLParser
# ---------------------------------------------------------------------------

ORACLE_DDL = """\
CREATE TABLE hr.employees (
    emp_id NUMBER(10) PRIMARY KEY NOT NULL,
    first_name VARCHAR2(50) NOT NULL,
    last_name VARCHAR2(50) NOT NULL,
    email VARCHAR2(100),
    hire_date DATE,
    salary NUMBER(10,2),
    dept_id NUMBER(10) REFERENCES departments(dept_id),
    status VARCHAR2(20) CHECK (status IN ('active','inactive','terminated'))
);

CREATE TABLE hr.departments (
    dept_id NUMBER(10) PRIMARY KEY NOT NULL,
    dept_name VARCHAR2(100) NOT NULL,
    location_id NUMBER(10)
);
"""

SQLSERVER_DDL = """\
CREATE TABLE [dbo].[Products] (
    [ProductID] INT IDENTITY(1,1) PRIMARY KEY,
    [ProductName] NVARCHAR(200) NOT NULL,
    [Price] MONEY NOT NULL,
    [CategoryID] INT,
    [IsActive] BIT DEFAULT 1
);

ALTER TABLE [dbo].[Products] ADD CONSTRAINT FK_Products_Categories
    FOREIGN KEY ([CategoryID]) REFERENCES [dbo].[Categories]([CategoryID]);
"""

POSTGRESQL_DDL = """\
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email CHARACTER VARYING(255),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true
);

CREATE TABLE orders (
    id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    total DECIMAL(10,2),
    order_date DATE
);
"""

MYSQL_DDL = """\
CREATE TABLE `users` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `username` VARCHAR(50) NOT NULL UNIQUE,
    `email` VARCHAR(255),
    `role` ENUM('admin','user','guest'),
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;
"""


class TestDDLParser:

    def test_can_parse(self):
        parser = DDLParser()
        assert parser.can_parse("CREATE TABLE foo (id INT);")
        assert not parser.can_parse("SELECT * FROM foo")

    def test_oracle_dialect_detected(self):
        parser = DDLParser()
        result = parser.parse(ORACLE_DDL)
        assert result.source_type == "oracle"

    def test_oracle_tables_extracted(self):
        result = DDLParser().parse(ORACLE_DDL)
        assert len(result.tables) == 2
        names = [t.name for t in result.tables]
        assert "employees" in names
        assert "departments" in names

    def test_oracle_schema_extracted(self):
        result = DDLParser().parse(ORACLE_DDL)
        emp = next(t for t in result.tables if t.name == "employees")
        assert emp.schema == "hr"

    def test_oracle_columns(self):
        result = DDLParser().parse(ORACLE_DDL)
        emp = next(t for t in result.tables if t.name == "employees")
        assert len(emp.columns) == 8
        col_names = [c.name for c in emp.columns]
        assert "emp_id" in col_names
        assert "salary" in col_names

    def test_oracle_type_normalization(self):
        result = DDLParser().parse(ORACLE_DDL)
        emp = next(t for t in result.tables if t.name == "employees")
        emp_id = next(c for c in emp.columns if c.name == "emp_id")
        assert emp_id.data_type == "decimal"  # NUMBER → decimal
        fname = next(c for c in emp.columns if c.name == "first_name")
        assert fname.data_type == "varchar"  # VARCHAR2 → varchar

    def test_oracle_pk(self):
        result = DDLParser().parse(ORACLE_DDL)
        emp = next(t for t in result.tables if t.name == "employees")
        emp_id = next(c for c in emp.columns if c.name == "emp_id")
        assert emp_id.is_primary_key

    def test_oracle_not_null(self):
        result = DDLParser().parse(ORACLE_DDL)
        emp = next(t for t in result.tables if t.name == "employees")
        fname = next(c for c in emp.columns if c.name == "first_name")
        assert not fname.nullable
        email = next(c for c in emp.columns if c.name == "email")
        assert email.nullable

    def test_oracle_check_constraint(self):
        result = DDLParser().parse(ORACLE_DDL)
        emp = next(t for t in result.tables if t.name == "employees")
        status = next(c for c in emp.columns if c.name == "status")
        assert set(status.check_constraints) == {"active", "inactive", "terminated"}

    def test_oracle_inline_fk(self):
        result = DDLParser().parse(ORACLE_DDL)
        emp = next(t for t in result.tables if t.name == "employees")
        assert len(emp.foreign_keys) == 1
        fk = emp.foreign_keys[0]
        assert fk.fk_column == "dept_id"
        assert fk.referenced_table == "departments"
        assert fk.referenced_column == "dept_id"

    def test_oracle_decimal_precision(self):
        result = DDLParser().parse(ORACLE_DDL)
        emp = next(t for t in result.tables if t.name == "employees")
        salary = next(c for c in emp.columns if c.name == "salary")
        assert salary.precision == 10
        assert salary.scale == 2

    def test_sqlserver_dialect_detected(self):
        result = DDLParser().parse(SQLSERVER_DDL)
        assert result.source_type == "sqlserver"

    def test_sqlserver_identity_pk(self):
        result = DDLParser().parse(SQLSERVER_DDL)
        products = result.tables[0]
        pid = next(c for c in products.columns if c.name == "ProductID")
        assert pid.is_primary_key

    def test_sqlserver_type_normalization(self):
        result = DDLParser().parse(SQLSERVER_DDL)
        products = result.tables[0]
        pname = next(c for c in products.columns if c.name == "ProductName")
        assert pname.data_type == "varchar"  # NVARCHAR → varchar
        price = next(c for c in products.columns if c.name == "Price")
        assert price.data_type == "decimal"  # MONEY → decimal
        active = next(c for c in products.columns if c.name == "IsActive")
        assert active.data_type == "boolean"  # BIT → boolean

    def test_sqlserver_alter_table_fk(self):
        result = DDLParser().parse(SQLSERVER_DDL)
        products = result.tables[0]
        assert len(products.foreign_keys) == 1
        fk = products.foreign_keys[0]
        assert fk.fk_column == "CategoryID"
        assert fk.referenced_table == "Categories"

    def test_postgresql_dialect_detected(self):
        result = DDLParser().parse(POSTGRESQL_DDL)
        assert result.source_type == "postgresql"

    def test_postgresql_serial_pk(self):
        result = DDLParser().parse(POSTGRESQL_DDL)
        customers = next(t for t in result.tables if t.name == "customers")
        cid = next(c for c in customers.columns if c.name == "id")
        assert cid.is_primary_key
        assert cid.data_type == "int"  # SERIAL → int

    def test_postgresql_bigserial(self):
        result = DDLParser().parse(POSTGRESQL_DDL)
        orders = next(t for t in result.tables if t.name == "orders")
        oid = next(c for c in orders.columns if c.name == "id")
        assert oid.data_type == "bigint"  # BIGSERIAL → bigint
        assert oid.is_primary_key

    def test_postgresql_type_normalization(self):
        result = DDLParser().parse(POSTGRESQL_DDL)
        customers = next(t for t in result.tables if t.name == "customers")
        email = next(c for c in customers.columns if c.name == "email")
        assert email.data_type == "varchar"  # CHARACTER VARYING → varchar

    def test_postgresql_inline_fk(self):
        result = DDLParser().parse(POSTGRESQL_DDL)
        orders = next(t for t in result.tables if t.name == "orders")
        assert len(orders.foreign_keys) == 1
        fk = orders.foreign_keys[0]
        assert fk.fk_column == "customer_id"
        assert fk.referenced_table == "customers"

    def test_mysql_dialect_detected(self):
        result = DDLParser().parse(MYSQL_DDL)
        assert result.source_type == "mysql"

    def test_mysql_auto_increment_pk(self):
        result = DDLParser().parse(MYSQL_DDL)
        users = result.tables[0]
        uid = next(c for c in users.columns if c.name == "id")
        assert uid.is_primary_key
        assert uid.data_type == "bigint"

    def test_mysql_unique(self):
        result = DDLParser().parse(MYSQL_DDL)
        users = result.tables[0]
        username = next(c for c in users.columns if c.name == "username")
        assert username.is_unique

    def test_mysql_enum_normalized(self):
        result = DDLParser().parse(MYSQL_DDL)
        users = result.tables[0]
        role = next(c for c in users.columns if c.name == "role")
        assert role.data_type == "varchar"  # ENUM → varchar

    def test_varchar_max_length(self):
        result = DDLParser().parse(POSTGRESQL_DDL)
        customers = next(t for t in result.tables if t.name == "customers")
        email = next(c for c in customers.columns if c.name == "email")
        assert email.max_length == 255

    def test_default_value_extracted(self):
        result = DDLParser().parse(POSTGRESQL_DDL)
        customers = next(t for t in result.tables if t.name == "customers")
        active = next(c for c in customers.columns if c.name == "is_active")
        assert active.default_value == "true"


# ---------------------------------------------------------------------------
# ETLMappingParser
# ---------------------------------------------------------------------------

ETL_CSV = """\
source_table,source_column,source_type,target_table,target_column,target_type,nullable,pk,fk_table,fk_column
customers,cust_id,NUMBER(10),customers,customer_id,INT,N,Y,,
customers,cust_name,VARCHAR2(100),customers,name,VARCHAR(100),N,N,,
customers,cust_email,VARCHAR2(255),customers,email,VARCHAR(255),Y,N,,
orders,order_id,NUMBER(10),orders,order_id,INT,N,Y,,
orders,cust_id,NUMBER(10),orders,customer_id,INT,N,N,customers,customer_id
orders,total_amt,NUMBER(10),orders,total,DECIMAL,Y,N,,
orders,order_dt,DATE,orders,order_date,DATE,N,N,,"""


class TestETLMappingParser:

    def test_can_parse(self):
        parser = ETLMappingParser()
        assert parser.can_parse(ETL_CSV)
        assert not parser.can_parse("CREATE TABLE foo (id INT);")

    def test_parse_csv(self):
        result = ETLMappingParser().parse(ETL_CSV)
        assert len(result.tables) == 2
        names = [t.name for t in result.tables]
        assert "customers" in names
        assert "orders" in names

    def test_target_side_columns(self):
        result = ETLMappingParser().parse(ETL_CSV, use_target_side=True)
        cust = next(t for t in result.tables if t.name == "customers")
        col_names = [c.name for c in cust.columns]
        assert "customer_id" in col_names  # target name, not source
        assert "name" in col_names

    def test_source_side_columns(self):
        result = ETLMappingParser().parse(ETL_CSV, use_target_side=False)
        cust = next(t for t in result.tables if t.name == "customers")
        col_names = [c.name for c in cust.columns]
        assert "cust_id" in col_names  # source name
        assert "cust_name" in col_names

    def test_pk_detected(self):
        result = ETLMappingParser().parse(ETL_CSV)
        cust = next(t for t in result.tables if t.name == "customers")
        pk = next(c for c in cust.columns if c.is_primary_key)
        assert pk.name == "customer_id"

    def test_nullable_detected(self):
        result = ETLMappingParser().parse(ETL_CSV)
        cust = next(t for t in result.tables if t.name == "customers")
        name = next(c for c in cust.columns if c.name == "name")
        assert not name.nullable
        email = next(c for c in cust.columns if c.name == "email")
        assert email.nullable

    def test_fk_detected(self):
        result = ETLMappingParser().parse(ETL_CSV)
        orders = next(t for t in result.tables if t.name == "orders")
        assert len(orders.foreign_keys) == 1
        fk = orders.foreign_keys[0]
        assert fk.fk_column == "customer_id"
        assert fk.referenced_table == "customers"
        assert fk.referenced_column == "customer_id"

    def test_type_extraction(self):
        result = ETLMappingParser().parse(ETL_CSV)
        orders = next(t for t in result.tables if t.name == "orders")
        total = next(c for c in orders.columns if c.name == "total")
        assert total.data_type == "decimal"

    def test_header_synonyms(self):
        # Use alternate headers
        csv = "src_table,src_col,src_type,tgt_table,tgt_col,tgt_type\nfoo,a,INT,bar,b,VARCHAR(50)\n"
        result = ETLMappingParser().parse(csv)
        assert len(result.tables) == 1
        assert result.tables[0].name == "bar"


# ---------------------------------------------------------------------------
# SchemaDefinitionParser
# ---------------------------------------------------------------------------


class TestSchemaDefinitionParser:

    def test_can_parse_json(self):
        parser = SchemaDefinitionParser()
        assert parser.can_parse('{"tables": []}')
        assert not parser.can_parse("random text")

    def test_simplified_json(self):
        schema = json.dumps({
            "tables": [{
                "name": "users",
                "columns": [
                    {"name": "id", "type": "int", "pk": True},
                    {"name": "name", "type": "varchar", "length": 100},
                    {"name": "status", "type": "varchar", "values": ["active", "inactive"]},
                ],
                "foreign_keys": [
                    {"column": "org_id", "references": "orgs", "references_column": "id"},
                ],
            }],
        })
        result = SchemaDefinitionParser().parse(schema)
        assert len(result.tables) == 1
        users = result.tables[0]
        assert len(users.columns) == 3
        uid = next(c for c in users.columns if c.name == "id")
        assert uid.is_primary_key
        assert uid.data_type == "int"
        status = next(c for c in users.columns if c.name == "status")
        assert status.check_constraints == ["active", "inactive"]
        assert len(users.foreign_keys) == 1
        assert users.foreign_keys[0].referenced_table == "orgs"

    def test_blueprint_format_passthrough(self):
        bp = json.dumps({
            "blueprint_id": "test-123",
            "source_system": {"name": "prod_oracle", "type": "oracle"},
            "tables": [{
                "name": "accounts",
                "columns": [{"name": "id", "data_type": "int"}],
                "foreign_keys": [],
            }],
        })
        result = SchemaDefinitionParser().parse(bp)
        assert result.source_name == "prod_oracle"
        assert result.source_type == "oracle"
        assert result.tables[0].name == "accounts"

    def test_string_column_shorthand(self):
        schema = json.dumps({
            "tables": [{
                "name": "t",
                "columns": ["id:int", "name:varchar", "email"],
            }],
        })
        result = SchemaDefinitionParser().parse(schema)
        cols = result.tables[0].columns
        assert cols[0].name == "id"
        assert cols[0].data_type == "int"
        assert cols[1].name == "name"
        assert cols[1].data_type == "varchar"
        assert cols[2].name == "email"
        assert cols[2].data_type == "varchar"  # default


# ---------------------------------------------------------------------------
# DefaultStats
# ---------------------------------------------------------------------------


class TestDefaultStats:

    def test_int_pk(self):
        col = ParsedColumn(name="id", data_type="int", is_primary_key=True, nullable=False)
        stats = generate_default_stats(col, row_count=1000)
        assert stats["null_ratio"] == 0.0
        assert stats["min"] == 1
        assert stats["max"] == 1000
        assert stats["distinct_count"] == 1000

    def test_int_non_pk(self):
        col = ParsedColumn(name="age", data_type="int")
        stats = generate_default_stats(col, row_count=1000)
        assert stats["null_ratio"] == 0.05
        assert stats["min"] == 1
        assert stats["max"] == 1000
        assert stats["mean"] == 500
        assert stats["stddev"] == 300

    def test_varchar(self):
        col = ParsedColumn(name="name", data_type="varchar", max_length=100)
        stats = generate_default_stats(col)
        assert stats["min_length"] == 3
        assert stats["max_length"] == 100

    def test_decimal_with_precision(self):
        col = ParsedColumn(name="price", data_type="decimal", precision=10, scale=2)
        stats = generate_default_stats(col)
        assert stats["min"] == 0.0
        assert stats["max"] <= 100_000_000  # 10^(10-2) - 1

    def test_date(self):
        col = ParsedColumn(name="created", data_type="date")
        stats = generate_default_stats(col)
        assert stats["min"] == "2020-01-01"
        assert stats["max"] == "2025-12-31"

    def test_boolean_default_true(self):
        col = ParsedColumn(name="active", data_type="boolean", default_value="true")
        stats = generate_default_stats(col)
        true_freq = next(v for v in stats["top_values"] if v["value"] is True)["frequency"]
        assert true_freq == 0.7

    def test_boolean_default_false(self):
        col = ParsedColumn(name="deleted", data_type="boolean", default_value="false")
        stats = generate_default_stats(col)
        true_freq = next(v for v in stats["top_values"] if v["value"] is True)["frequency"]
        assert true_freq == 0.3

    def test_check_constraint_as_categorical(self):
        col = ParsedColumn(
            name="status", data_type="varchar",
            check_constraints=["pending", "shipped", "delivered"],
        )
        stats = generate_default_stats(col)
        assert len(stats["top_values"]) == 3
        assert stats["distinct_count"] == 3
        assert abs(stats["top_values"][0]["frequency"] - 1 / 3) < 0.01

    def test_not_null_zero_null_ratio(self):
        col = ParsedColumn(name="x", data_type="int", nullable=False)
        stats = generate_default_stats(col)
        assert stats["null_ratio"] == 0.0

    def test_nullable_has_null_ratio(self):
        col = ParsedColumn(name="x", data_type="int", nullable=True)
        stats = generate_default_stats(col)
        assert stats["null_ratio"] == 0.05

    def test_bigint(self):
        col = ParsedColumn(name="big_id", data_type="bigint")
        stats = generate_default_stats(col)
        assert stats["max"] == 100_000

    def test_timestamp(self):
        col = ParsedColumn(name="ts", data_type="timestamp")
        stats = generate_default_stats(col)
        assert "min" in stats
        assert "max" in stats

    def test_unknown_type_fallback(self):
        col = ParsedColumn(name="x", data_type="geometry")
        stats = generate_default_stats(col)
        assert "min_length" in stats  # fallback to string


# ---------------------------------------------------------------------------
# BlueprintAssembler
# ---------------------------------------------------------------------------


class TestBlueprintAssembler:

    def _make_parsed_schema(self):
        return ParsedSchema(
            source_name="Test Oracle",
            source_type="oracle",
            tables=[
                ParsedTable(
                    name="customers",
                    schema="hr",
                    columns=[
                        ParsedColumn(name="id", data_type="int", nullable=False, is_primary_key=True),
                        ParsedColumn(name="name", data_type="varchar", nullable=False, max_length=100),
                        ParsedColumn(name="email", data_type="varchar", max_length=255),
                    ],
                ),
                ParsedTable(
                    name="orders",
                    schema="hr",
                    columns=[
                        ParsedColumn(name="id", data_type="int", nullable=False, is_primary_key=True),
                        ParsedColumn(name="customer_id", data_type="int", nullable=False),
                        ParsedColumn(name="total", data_type="decimal", precision=10, scale=2),
                        ParsedColumn(name="status", data_type="varchar", check_constraints=["pending", "shipped"]),
                    ],
                    foreign_keys=[
                        ParsedForeignKey(fk_column="customer_id", referenced_table="customers", referenced_column="id"),
                    ],
                ),
            ],
            input_format=InputFormat.DDL,
        )

    def test_blueprint_structure(self):
        bp = BlueprintAssembler().assemble(self._make_parsed_schema())
        assert "blueprint_id" in bp
        assert "source_system" in bp
        assert "tables" in bp
        assert "relationships" in bp
        assert "security_model" in bp

    def test_source_system(self):
        bp = BlueprintAssembler().assemble(self._make_parsed_schema())
        assert bp["source_system"]["name"] == "Test Oracle"
        assert bp["source_system"]["type"] == "oracle"

    def test_table_count(self):
        bp = BlueprintAssembler().assemble(self._make_parsed_schema())
        assert len(bp["tables"]) == 2

    def test_column_stats_present(self):
        bp = BlueprintAssembler().assemble(self._make_parsed_schema())
        cust = next(t for t in bp["tables"] if t["name"] == "customers")
        for col in cust["columns"]:
            assert "stats" in col
            assert "null_ratio" in col["stats"]

    def test_fk_key_names_match_relationship_preserver(self):
        """FK keys must use 'column', 'references_table', 'references_column'
        to match RelationshipPreserver.get_fk_constraints() at line 77."""
        bp = BlueprintAssembler().assemble(self._make_parsed_schema())
        orders = next(t for t in bp["tables"] if t["name"] == "orders")
        assert len(orders["foreign_keys"]) == 1
        fk = orders["foreign_keys"][0]
        assert "column" in fk
        assert "references_table" in fk
        assert "references_column" in fk
        assert fk["column"] == "customer_id"
        assert fk["references_table"] == "customers"
        assert fk["references_column"] == "id"

    def test_relationships_built(self):
        bp = BlueprintAssembler().assemble(self._make_parsed_schema())
        assert len(bp["relationships"]) == 1
        rel = bp["relationships"][0]
        assert "orders" in rel["from_table"]
        assert "customers" in rel["to_table"]

    def test_row_count_override(self):
        bp = BlueprintAssembler().assemble(self._make_parsed_schema(), row_count=500)
        for t in bp["tables"]:
            assert t["row_count"] == 500

    def test_pk_stats_correct(self):
        bp = BlueprintAssembler().assemble(self._make_parsed_schema(), row_count=1000)
        cust = next(t for t in bp["tables"] if t["name"] == "customers")
        id_col = next(c for c in cust["columns"] if c["name"] == "id")
        assert id_col["stats"]["min"] == 1
        assert id_col["stats"]["max"] == 1000
        assert id_col["stats"]["distinct_count"] == 1000

    def test_check_constraint_becomes_top_values(self):
        bp = BlueprintAssembler().assemble(self._make_parsed_schema())
        orders = next(t for t in bp["tables"] if t["name"] == "orders")
        status = next(c for c in orders["columns"] if c["name"] == "status")
        assert "top_values" in status["stats"]
        values = [tv["value"] for tv in status["stats"]["top_values"]]
        assert set(values) == {"pending", "shipped"}

    def test_blueprint_compatible_with_distribution_sampler(self):
        """Verify assembled blueprint can be consumed by DistributionSampler."""
        from dais_seg.generator.distribution_sampler import DistributionSampler
        bp = BlueprintAssembler().assemble(self._make_parsed_schema(), row_count=100)
        sampler = DistributionSampler(seed=42)
        for table in bp["tables"]:
            for col in table["columns"]:
                values = sampler.sample_column(col, table["row_count"])
                assert len(values) == table["row_count"]

    def test_blueprint_compatible_with_relationship_preserver(self):
        """Verify assembled blueprint works with RelationshipPreserver."""
        from dais_seg.generator.relationship_preserver import RelationshipPreserver
        bp = BlueprintAssembler().assemble(self._make_parsed_schema())
        preserver = RelationshipPreserver(bp)
        order = preserver.get_generation_order()
        # customers must come before orders
        assert order.index("customers") < order.index("orders")
        fks = preserver.get_fk_constraints("orders")
        assert len(fks) == 1
        assert fks[0]["child_column"] == "customer_id"
        assert fks[0]["parent_table"] == "customers"


# ---------------------------------------------------------------------------
# End-to-end: DDL → Blueprint → Sampler
# ---------------------------------------------------------------------------


class TestEndToEnd:

    def test_ddl_to_sampled_data(self):
        """Parse DDL → assemble blueprint → sample all columns."""
        from dais_seg.generator.distribution_sampler import DistributionSampler

        parsed = DDLParser().parse(POSTGRESQL_DDL)
        bp = BlueprintAssembler().assemble(parsed, row_count=50)
        sampler = DistributionSampler(seed=99)

        for table in bp["tables"]:
            for col in table["columns"]:
                values = sampler.sample_column(col, table["row_count"])
                assert len(values) == 50

    def test_etl_to_blueprint(self):
        """Parse ETL CSV → assemble blueprint → verify structure."""
        parsed = ETLMappingParser().parse(ETL_CSV)
        bp = BlueprintAssembler().assemble(parsed, row_count=100)
        assert len(bp["tables"]) == 2
        assert len(bp["relationships"]) == 1
