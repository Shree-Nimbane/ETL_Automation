import pytest
import json
import os
import yaml
from pyspark.sql.types import *
from delta import *
from pyspark.sql.functions import lit, concat
from pyspark.sql import functions as F
from delta.tables import *
from pyspark.sql.functions import xxhash64

from operator import and_


@pytest.mark.usefixtures('red_config', 'spark_session')
class BaseClass:

    @property
    def read_yml(self):
        xpath = self.path
        # config_path = xpath + '\\config.yml'
        config_path = os.path.join(xpath, 'config.yml')
        with open(config_path, "r") as ts:
            value = yaml.safe_load(ts)
        return value

    def read_db(self, config):
        cred = self.load_credentials()
        spark = self.spark
        cred_lookup = config['cred_lookup']
        creds = cred[cred_lookup]
        # print(creds)
        df = spark.read.format('jdbc'). \
            option('url', creds['url']). \
            option('user', creds['user']). \
            option('password', creds['password']). \
            option('database', creds['database']). \
            option('schema', creds['schema']). \
            option('dbtable', creds['dbtable']). \
            option('driver', creds['driver']). \
            load()
        return df

    @property
    def read_data(self):
        config_data = self.read_yml
        spark = self.spark
        source_config = self.read_yml['source']
        target_config = self.read_yml['target']
        validations_config = self.read_yml['validations']
        #
        if source_config['type'] == 'database':
            source_df = self.read_db(config=source_config)
        else:
            # df = spark.read.json(source_config['path'],multiLine=True)
            source_df = self.read_file(config=source_config)

        if target_config['type'] == 'database':
            target_df = self.read_db(config=target_config)
        else:
            target_df = self.read_file(config=target_config)

        return source_df, target_df

    #
    def read_file(self, config):
        file_path = config['path']
        spark = self.spark
        file_type = config['type'].lower()
        df = None
        if file_type == 'csv':
            if config['schema'].lower() == 'y':
                spark.read.csv(file_path, schema=self.read_schema, sep=config['options']['delimiter'],
                               header=config['options']['header'])
            else:
                df = spark.read.csv(file_path, sep=config['options']['delimiter'],
                                    header=config['options']['header'],inferSchema=True)
        elif file_type == 'json':
            if config['schema'].lower() == 'y':
                schema = self.read_schema
                df = spark.read.schema(schema).json(file_path, multiLine=config['options']['multiline'], schema=schema)
            else:
                df = spark.read.json(file_path, multiLine=config['options']['multiline'])
        elif file_type == 'parquet':
            df = spark.read.parquet(file_path)
        elif file_type == 'avro':
            df = spark.read.format('avro').load(file_path)
        elif type == 'text':
            df = spark.read.csv(file_path)

        return df

    @property
    def read_schema(self):
        schema_path = os.path.join(self.path, 'schema.json')
        with open(schema_path, 'r') as schema_file:
            schema = StructType.fromJson(json.load(schema_file))
        return schema

    def load_credentials(self, env='qa'):
        # taf_path='.\\cred_config.yml'
        taf_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        credentials_path = os.path.join(taf_path, 'project_config', 'cred_config.yml')
        with open(credentials_path, 'r') as file:
            credentials = yaml.safe_load(file)
            # print(credentials[env])
        return credentials[env]