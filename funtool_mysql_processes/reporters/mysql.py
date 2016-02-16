# Reporter Process to output states and groups to a MySQL database

import funtool.reporter
import funtool.lib.general

import pymysql
import pymysql.cursors

import yaml
import os

def insert_states(reporter,state_collection,overriding_parameters=None,logging=None):
    """
    Inserts state values into a db

    Must have db_table, and db_connection parameters (can optionally specify a YAML config_file) 

    Values to be saved are listed in the values dict, each key is the db_table column, each value is a state's field: value_name 
    
    Example
    -------
    reporter_module: funtool_common_processes.mysql
    reporter_function: insert_states
    parameters:
        db_connection:
            config_file: ../../database/connection.yml
        db_table: users
        values:
            game_id: 
                meta: user_id
            score:
                measures: total_score
        

    """
    reporter_parameters= funtool.reporter.get_parameters(reporter,overriding_parameters)
    connection= _open_connection(_connection_values(reporter_parameters.get('db_connection')))
    try:
        for state in state_collection.states:
            state_values= []
            for (db_column,state_value) in reporter_parameters.get('values',{}).items():
                state_value_tuple= funtool.lib.general.get_tuple(state_value)
                state_values.append( (db_column, getattr(state, state_value_tuple[0]).get(state_value_tuple[1])) )
            _write_values( connection, reporter_parameters.get('db_table'), state_values )
    finally:
        connection.close()
        


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
    conn_parameters['cursorclass']= pymysql.cursors.DictCursor  # Forces the cursor to return Dicts
    return conn_parameters
     

def _write_values( connection, table_name, values_list): #values_list is a list of db_column: db_value tuples
    with connection.cursor() as cursor:
        column_names= ' , '.join([ '`'+str(db_column)+'`' for (db_column, db_value) in values_list ])  
        column_values= ' , '.join([ "'"+str(db_value)+"'" for (db_column, db_value) in values_list ])  
        sql="INSERT INTO `{}` ( {} ) VALUES ( {} )".format(table_name, column_names, column_values)
        cursor.execute(sql)
        connection.commit()
     


