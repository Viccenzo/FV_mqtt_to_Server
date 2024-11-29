"""import pandas as pd
#import sqlalchemy
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    DateTime,
    exists,
    inspect,
    text,
    Integer, 
    String, 
    Float, 
    Boolean
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import VARCHAR
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.dialects.postgresql import insert
import os
import dotenv
import paho.mqtt.client as mqtt
import time
from datetime import datetime
#from sqlalchemy import insert
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import SQLAlchemyError"""

import os
import pandas as pd
import dotenv
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, DateTime, Boolean, VARCHAR, inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError
import time
import datetime
import json

# Function to create DB engine
def createEngine(user,passwd,server,database):    
    engine = create_engine(f'postgresql://{user}:{passwd}@{server}/{database}', 
                           echo=False,
                           pool_size=20,          # Aumenta o número de conexões no pool
                           max_overflow=40,       # Permite até 20 conexões adicionais
                           pool_timeout=60,       # Aumenta o tempo de espera por uma conexão
                           pool_recycle=3600,     # Tempo de reciclagem das conexões em segundos
                           connect_args={'application_name': 'MQTT_BACKEND_WORKER1'}
                           )
    return engine

def convert_to_numeric(df):
    for column in df.columns:
        if column != 'TIMESTAMP':  # Ignorar a coluna TIMESTAMP
            try:
                # Tenta converter a coluna para numérico
                df[column] = pd.to_numeric(df[column])
            except (ValueError, TypeError):
                # Se houver erro, mantém a coluna como está
                pass

# Function to map types from pandas to SQLalchey
def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Float
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return DateTime
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    else:
        return VARCHAR

""" def insert_dataframe(engine, table_name, dataframe):

    # Convert dataframe to dictionary format
    records = dataframe.to_dict(orient='records')
    
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    with engine.connect() as conn:
        with conn.begin():
            for record in records:
                stmt = insert(table).values(record)
                stmt = stmt.on_conflict_do_nothing(index_elements=['timestamp'])  # Adjust this for your primary key column(s)
                conn.execute(stmt) """



def uploadToDB(engine, dataframe, table_name):
    records = dataframe.to_dict(orient='records')
    
    # Carrega os metadados da tabela
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    try:
        # Usando transação manualmente com a engine
        with engine.connect() as conn:
            with conn.begin():
                # Inserção em massa com tratamento de conflito
                stmt = insert(table).values(records)

                # Evitar conflitos com chaves duplicadas na coluna 'TIMESTAMP'
                stmt = stmt.on_conflict_do_nothing(index_elements=['TIMESTAMP'])

                # Executa a inserção em lote com tratamento de conflito
                conn.execute(stmt)
        
        print(f"Data successfully uploaded to {table_name} table!")
        return f"Data successfully uploaded to {table_name} table!"

    except SQLAlchemyError as e:
        # Captura qualquer erro relacionado ao SQLAlchemy e printa
        print(f"An error occurred during the database operation: {e}")
        return f"An error occurred during the database operation on table {table_name}: {e}"

# Function to check if a table exists inside the database
def tableExists(tableName, engine, schemaName='public'):
    query = text('''
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema_name
            AND table_name = :table_name
        );
    ''')
    
    with engine.connect() as conn:
        # Execute the query and pass the parameters
        result = conn.execute(query, {"schema_name": schemaName, "table_name": tableName}).fetchone()
        
        # Extract the boolean result from the query
        exists = result[0]  # This will give you True/False for the EXISTS clause
    
    return exists

# function to create a table on database
def createTable(dataFrame, engine, tableName, column_types):
    print(column_types)
    metadata = MetaData()
    table = Table(tableName, metadata, *(Column(name, column_types[name]) for name in column_types))
    metadata.create_all(engine)

# function that compare header between dataframe and databse to extract diferences
def headerMismach(tableName, engine, dataFrame):
    inspector = inspect(engine)
    columns = inspector.get_columns(tableName)
    column_names = [column['name'] for column in columns]
    headers = dataFrame.columns.tolist()
    missing_in_db = set(headers) - set(column_names)
    return missing_in_db

# function that adds missing columns present on pandas dataframe
def addMissingColumn(missing_columns,engine,table_name,dataFrame):
    metadata = MetaData()
    for column_name in missing_columns:
        column_types = {name: map_dtype(dtype) for name, dtype in dataFrame.dtypes.items()}
        column_type = column_types[column_name]
        alter_statement = text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type().compile(dialect=engine.dialect)}')
        try:
            with engine.connect() as conn:
                with conn.begin():
                    conn.execute(alter_statement)
            print(f"column '{column_name}' sucessfully added")
        except ProgrammingError as e:
            print(f"Column creation error '{column_name}': {e}")

def primaryKeyExists(engine, tableName):
    metadata = MetaData()
    inspector = inspect(engine)
    primary_keys = inspector.get_pk_constraint(tableName)['constrained_columns']
    return primary_keys

def addPrimarykey(engine, tableName, keyName):
    alter_statement = text(f'ALTER TABLE "{tableName}" ADD PRIMARY KEY ("{keyName}")')
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(alter_statement)
    except ProgrammingError as e:
        print(f"Set primary key error: {e}")

def getTableUsers(engine, tableName):
    fetchStatement = text(f'SELECT "user" FROM "tables" WHERE "table" = \'{tableName}\'')
    #print(fetchStatement)
    try:
        with engine.connect() as conn:
            with conn.begin():
                result = conn.execute(fetchStatement)
                tableOwners = result.fetchall()
                #print(tableOwners)
                tableOwnerStrings = [owner[0] for owner in tableOwners]
                return tableOwnerStrings[0].split(",")
    except ProgrammingError as e:
        print(f"Query execution error: {e}")
        return []

def createTableUser(engine,table,user):
    create_statement = text(f'INSERT INTO "tables" ("table", "user") VALUES (\'{table}\', \'{user}\');')
    #print(create_statement)
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(create_statement)
    except ProgrammingError as e:
        print(f"Set primary key error: {e}")

def getRecentTimestamp(engine, tableName, user):
    query = text(f'''
        SELECT "TIMESTAMP" FROM "{tableName}"
        ORDER BY "TIMESTAMP" DESC
        LIMIT 1
    ''')
    
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            recent_row = result.fetchone()
            if recent_row:
                print(f"Most recent row in table {tableName} for user {user}: {recent_row}")
                return recent_row
            else:
                print(f"No data found in table {tableName} for user {user}.")
                return None
    except ProgrammingError as e:
        print(f"Error fetching most recent row: {e}")
        return None

def createMqttConnection(mqtt_broker,mqtt_port,topic,dbConnections):
    client = mqtt.Client(userdata=dbConnections)
    client.connect(mqtt_broker, mqtt_port)
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_start()
    
def on_connect(client, userdata, flags, rc):
    print(f"Conectado com o código de resultado {rc}")
    client.subscribe("DB_INSERT/#")  # Subscrição ao tópico passado via userdata
    client.subscribe("DB_GERT_RECENT_ROW/#")  # Subscrição ao tópico passado via userdata

def createStatus(created_at, table_name, status, message, start_time, end_time, total_running_time, logger_start_time, logger_end_time, logger_time, mqtt_start_time, mqtt_end_time, mqtt_time, db_start_time, db_end_time, db_time, user):
    # Definindo as colunas e seus tipos
    data = {
        'created_at': [created_at],  # timestamp
        'Table_name': [table_name],  # text
        'Status': [status],  # text
        'Message': [message],  # text
        'start_time': [start_time],  # timestamp
        'end_time': [end_time],  # timestamp
        'total_running_time': [total_running_time],  # double precision
        'logger_start_time':  [logger_start_time], 
        'logger_end_time': [logger_end_time], 
        'logger_time': [logger_time], 
        'mqtt_start_time': [mqtt_start_time], 
        'mqtt_end_time': [mqtt_end_time], 
        'mqtt_time': [mqtt_time], 
        'db_start_time': [db_start_time], 
        'db_end_time': [db_end_time], 
        'db_time': [db_time],
        'user': [user]

    }

    # Criando o DataFrame
    df = pd.DataFrame(data)

    return df

def on_message(client, userdata, msg):
    print(f"Mensagem recebida no tópico {msg.topic}")
    mqttArivalTime = datetime.datetime.now()
    try:
        command,user,tableName = msg.topic.split("/")
    except Exception as e:
        print(e)
        client.publish(f'message/{user}/{tableName}', "Missing topic information on FV_mqtt_to_Server code ", qos=1)
        return
    
    if command == "DB_INSERT":
        try:    
            data = json.loads(msg.payload.decode())
            df = pd.read_json(data['df_data'])
            convert_to_numeric(df)
        except Exception as e:
            print(e)
            client.publish(f'message/{user}/{tableName}', f'error decoding mqtt to dataframe: {e}', qos=1)
            return
        if 'TIMESTAMP' not in df:
            print("Missing TIMESTAMP on Header")
            client.publish(f'message/{user}/{tableName}', f'Missing TIMESTAMP on Header', qos=1)
            return

        try:
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        except:
            print("Error parsing timestamp to SQL format")
            client.publish(f'message/{user}/{tableName}', f'Error parsing timestamp to SQL format', qos=1)
            return
        try:
            column_types = {name: map_dtype(dtype) for name, dtype in df.dtypes.items()}
        except:
            print("Error parsing dataframe data to SQL format")
            client.publish(f'message/{user}/{tableName}', f'Error parsing dataframe data to SQL format', qos=1)
            return

        for engine in userdata:
            try:
                #Check if table exist and create one otherwise (rethink this)
                if not tableExists(tableName,userdata[0]):
                    print('The table does not exist, creating table')
                    createTable(df, engine, tableName,column_types)
                    createTableUser(engine,tableName,user)
                    primaryKey = primaryKeyExists(engine,tableName)
                    if primaryKey == []:
                        print(primaryKey)
                        addPrimarykey(engine,tableName,'TIMESTAMP') # for now the only primary key is going to be timestamp, changein the future
                
                """
                #Check if person is owner of the table (future)
                owners = getTableUsers(engine,tableName)
                if not owners:
                    client.publish(f'message/{user}/{tableName}', f'Table exists but have no owners. Please Contact DB administrator', qos=1)
                    return
                isOwner = 0
                for owner in owners:
                    if owner == user:
                        isOwner = 1
                if isOwner == 0:
                    client.publish(f'message/{user}/{tableName}', f'You dont have permission to insert data in this table, contact system administrator', qos=1)
                    return
                """

                # Check for missmach on headers
                missmach = headerMismach(tableName,engine,df)
                if(len(missmach) != 0):
                    addMissingColumn(missmach,engine,tableName,df)
                    client.publish(f'message/{user}/{tableName}', f'The header you are providing doesent match the server headres. Those are the headers created: {missmach}', qos=1)
                    return

                #upload to database
                print("uploading data to database")
                print(df)
                dataBaseStartUploadTime = datetime.datetime.now()
                dbMessage = uploadToDB(engine,df,tableName)
                dataBaseEndUploadTime = datetime.datetime.now()
                client.publish(f'message/{user}/{tableName}', dbMessage, qos=1)
                statusDF = createStatus(dataBaseEndUploadTime,
                                        tableName, 
                                        "Success", 
                                        "Data inserted successfully", 
                                        datetime.datetime.fromisoformat(data['loggerRequestBeginTime']), 
                                        dataBaseEndUploadTime, 
                                        (dataBaseEndUploadTime-datetime.datetime.fromisoformat(data['loggerRequestBeginTime'])).total_seconds(),
                                        datetime.datetime.fromisoformat(data['loggerRequestBeginTime']),
                                        datetime.datetime.fromisoformat(data['loggerRequestEndTime']),
                                        (datetime.datetime.fromisoformat(data['loggerRequestEndTime'])-datetime.datetime.fromisoformat(data['loggerRequestBeginTime'])).total_seconds(),
                                        datetime.datetime.fromisoformat(data['loggerRequestEndTime']),
                                        mqttArivalTime,
                                        (mqttArivalTime-datetime.datetime.fromisoformat(data['loggerRequestEndTime'])).total_seconds(),
                                        dataBaseStartUploadTime,
                                        dataBaseEndUploadTime,
                                        (dataBaseEndUploadTime-dataBaseStartUploadTime).total_seconds(),
                                        user
                                        )
                #criar uma função depois
                records = statusDF.to_dict(orient='records')
                print(records)
    
                metadata = MetaData()
                table = Table("TABLES_RUNNING_STATUS", metadata, autoload_with=engine)
                
                with engine.connect() as conn:
                    with conn.begin():
                        # Inserção em massa com tratamento de conflito
                        stmt = insert(table).values(records)

                        # Executa a inserção em lote com tratamento de conflito
                        conn.execute(stmt)
            except Exception as e:
                print(e)
                client.publish(f'message/{user}/{tableName}', f'error while treating data to upload to DB: {e}', qos=1)
    
    if command == "DB_GERT_RECENT_ROW":
        engine = userdata[0]

        #Check if table exist and create one otherwise (rethink this)
        if not tableExists(tableName,userdata[0]):
            client.publish(f'message/{user}/{tableName}', f'Table doesent exist', qos=1)
            return

        response = getRecentTimestamp(engine,tableName,user)

        """
        #Check if person is owner of the table (future)
        owners = getTableUsers(engine,tableName)
        if not owners:
            client.publish(f'message/{user}/{tableName}', f'Table exists but have no owners. Please Contact DB administrator', qos=1)
            return
        isOwner = 0
        for owner in owners:
            if owner == user:
                isOwner = 1
        if isOwner == 0:
            client.publish(f'message/{user}/{tableName}', f'You dont have permission to insert data in this table, contact system administrator', qos=1)
            return
        """
        
        client.publish(f'message/{user}/{tableName}', f'{response}', qos=1)
 

def main():

    dotenv.load_dotenv()
    
    servers = os.getenv("POSTGRESQL_SERVERS").split(';')

    serverConnections = []

    for server in servers:
        arguments = server.split(',')
        serverConnections.append(createEngine(arguments[0],arguments[1],arguments[2],arguments[3]))

    #Connecting to MQTT Brokers
    brokers = os.getenv("MQTT_BROKERS").split(';')

    for broker in brokers:
        arguments = broker.split(",")
        createMqttConnection(arguments[0],int(arguments[1]),arguments[2],serverConnections)

    while(1):
        time.sleep(1)

main()