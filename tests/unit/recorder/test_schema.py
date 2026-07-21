# -*- coding: utf-8 -*-
import pytest
from EC_API.recorders.base import SQLSchemaTable

def test_sql_schema_init_valid() -> None:
    ST = SQLSchemaTable(
        table_name = "test_table",
        columns = (
            ('field_0', 'ANY'), 
            ('field_1', 'INTEGER'), 
            ('field_2', 'REAL'), 
            ('field_3', 'TEXT'), 
            ('field_4', 'BLOB')
        )
        )
    
    assert ST.table_name == "test_table"
    assert ST.columns == (
        ('field_0', 'ANY', ''), 
        ('field_1', 'INTEGER', ''), 
        ('field_2', 'REAL', ''), 
        ('field_3', 'TEXT', ''), 
        ('field_4', 'BLOB', '')
    )
    
    create_q = ST.create_query()    
    assert create_q == "CREATE TABLE IF NOT EXISTS test_table (\n field_0 ANY,\nfield_1 INTEGER,\nfield_2 REAL,\nfield_3 TEXT,\nfield_4 BLOB\n)"
    
    insert_q_sqlite3 = ST.insert_query("sqlite3")
    assert insert_q_sqlite3 == "INSERT INTO test_table (field_0, field_1, field_2, field_3, field_4) VALUES (?, ?, ?, ?, ?)"
    
    insert_q_asyncpg = ST.insert_query("asyncpg")
    assert insert_q_asyncpg == "INSERT INTO test_table (field_0, field_1, field_2, field_3, field_4) VALUES ($1, $2, $3, $4, $5)"
    
    insert_q_psycopg = ST.insert_query('psycopg')
    assert insert_q_psycopg == "INSERT INTO test_table (field_0, field_1, field_2, field_3, field_4) VALUES (%s, %s, %s, %s, %s)"
    
    insert_q_psycopg = ST.insert_query('pymysql')
    assert insert_q_psycopg == "INSERT INTO test_table (field_0, field_1, field_2, field_3, field_4) VALUES (%s, %s, %s, %s, %s)"
    
    insert_q_psycopg = ST.insert_query('mysqlclient')
    assert insert_q_psycopg == "INSERT INTO test_table (field_0, field_1, field_2, field_3, field_4) VALUES (%s, %s, %s, %s, %s)"
    
    
def test_sql_schema_wrong_type_in_column_invalid() -> None:
    with pytest.raises(TypeError):
        SQLSchemaTable(
            table_name = "test_table",
            columns = (
                ('field_0', 'ANY'), 
                (111, 'INTEGER'),  #<-- wrong type
            ))
        
def test_sql_schema_wrong_data_type_invalid() -> None:
    with pytest.raises(ValueError):
        SQLSchemaTable(
            table_name = "test_table",
            columns = (
                ('field_0', 'ANY'), 
                ('field_1', 'JUNK'),  #<-- wrong type
            ))

def test_sql_schema_() -> None:
    ST = SQLSchemaTable(
        table_name = "test_table",
        columns = (
            ('field_0', 'ANY'), 
            ('field_1', 'INTEGER'), 
            ('field_2', 'REAL'), 
            ('field_3', 'TEXT'), 
            ('field_4', 'BLOB')
        )
        )
    with pytest.raises(ValueError):
        ST.insert_query("wrong_db")
