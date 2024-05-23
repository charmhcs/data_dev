from typing import Sequence
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import awswrangler as wr
import psycopg2
import logging
import pandas as pd

class AthenaToPostgresOperator(BaseOperator):
    """
        Atehan query 결과값을 postgresql에 insert 함
    """
    template_fields: Sequence[str] = ('athena_query',)
    template_ext: Sequence[str] = ('.sql', '.hql')
    template_fields_renderers = {"athena_query": "sql"}

    def __init__(self,
                 athena_query: str,
                 athena_database: str = 'default',
                 aws_conn_id: str = 'aws_default',
                 postgres_conn_id: str = None,
                 postgres_schema: str = None,
                 postgres_table: str= None,
                 index: bool = False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.athena_query = athena_query
        self.athena_database = athena_database
        self.aws_conn_id = aws_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.postgres_schema = postgres_schema
        self.postgres_table = postgres_table
        self.index = index

    def _insert_postgres(self, conn, df: pd.DataFrame):
        tuples= list(df.to_records(index = self.index))
        cols = ','.join(list(df.columns))
        # SQL query to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (self.postgres_table, cols)
        cursor = conn.cursor()
        try:
            psycopg2.extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            cursor.close()
            raise Exception("Error: %s" % error)
        cursor.close()
        logging.info("the dataframe is inserted")


    def execute(self, context):
        logging.info(self.athena_query)
        df = wr.athena.read_sql_query(
            sql = self.athena_query,
            database = self.athena_database
        )
        conn = PostgresHook(postgres_conn_id = self.postgres_conn_id, schema = self.postgres_schema).get_conn()
        self._insert_postgres(conn, df)