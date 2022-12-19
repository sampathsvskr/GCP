## https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/operators/bigquery.html#BigQueryInsertJobOperator

# We are updating the BigQueryInsertJobOperator
'''
1) Get the sql file which contains params from dags gcs bucket of airflow
2)Update thw params with values
3)Run the query in BigQuery
'''
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowException
import re

class BigQueryCustomExecuteQueryOperator(BaseOperator):
    def __init__(
                self,
                gcp_conn_id : str  ,
                sql_file_path : str = None,
                query :str =None , 
                sql_params : dict = {}, 
                use_legacy_sql : bool = False, 
                **kwargs
                ) -> None:
        
        if not sql_file_path and not query:
            raise AirflowException("Provide either sql_file_path or query")

        super().__init__(**kwargs)        
        self.sql_file_path = sql_file_path
        self.query = query
        self.sql_params = sql_params
        self.use_legacy_sql =use_legacy_sql
        self.gcp_conn_id = gcp_conn_id


    def read_sql_file(self) -> str:
        with open(self.sql_file_path) as f:
            query = f.read()
        return query
    
    def update_query_params(self , query) -> str:
        for param in re.findall(r'\{.*?\}', query):
            param = re.sub(r'[\{\}]','',i)
            query = query.replace(f'{{{param}}}', params[param])

        return query

    def execute(self , context):
        query=""
        if self.sql_file_path:
            query = self.read_sql_file()
        else:
            query = self.query

        sql_query = self.update_query_params(self, query) 
        print(sql_query)       

        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            use_legacy_sql = self.use_legacy_sql,
        )
        result = hook.run_query(sql_query)
        return "Query executed successfully"




