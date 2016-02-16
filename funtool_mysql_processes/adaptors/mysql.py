# Adaptors to import tabular data from mysql

import funtool.adaptor
import funtool.state_collection
import funtool.lib.general
import funtool_common_processes.adaptors.tabular

import pymysql
import pymysql.cursors

import yaml
import os

def mysql_statement_import(adaptor,state_collection,overriding_parameters=None,logging=None):
    """
    Inserts state values into a db

    Must have db_table, and db_connection parameters (can optionally specify a YAML config_file) 

    Values to be saved are listed in the values dict, each key is the db_table column, each value is a state's field: value_name 
    
    Example
    -------
    adaptor_module: funtool_common_processes.mysql
    adaptor_function: mysql_statement_import
    parameters:
        db_connection:
            config_file: ../../database/connection.yml
        SQL: "SELECT * FROM users"
        tabular_parameters:
            game_id: 
                meta: user_id
            score:
                measures: total_score
        

    """
    adaptor_parameters= funtool.adaptor.get_adaptor_parameters(adaptor,overriding_parameters)
    connection= _open_connection(_connection_values(adaptor_parameters.get('db_connection')))
    try:
        reader= _mysql_row(connection, adaptor_parameters.get('SQL'))
        new_state_collection= funtool_common_processes.adaptors.tabular.create_from_config(reader, adaptor_parameters.get('tabular_parameters',{}))
    finally:
        connection.close()
    
    return funtool.state_collection.join_state_collections(state_collection, new_state_collection )

def _open_connection(connection_values):
    return pymysql.connect( **connection_values)


def _connection_values(adaptor_db_connection_parameters):
    conn_parameters= adaptor_db_connection_parameters.copy()
    config_file= conn_parameters.pop('config_file',None)
    yaml_config= None
    if not config_file is None:
        with open(config_file) as f:
            yaml_config= yaml.load(f)
    if yaml_config is None:
        yaml_config= {}
    yaml_config.update(conn_parameters)
    conn_parameters= yaml_config
    return conn_parameters

def _mysql_row(connection, sql):
    with connection.cursor() as cur:
        cur.execute(sql)
        results= cur.fetchall()
        yield [ col[0] for col in cur.description ]  #include column headers
        for result in results:
            yield result
