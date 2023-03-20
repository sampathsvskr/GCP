## https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/operators/bigquery.html#BigQueryInsertJobOperator

# Create a custom operator with reference to BigQueryInsertJobOperator
'''
1) Get the sql file which contains params from dags gcs bucket of airflow
2)Update thw params with values
3)Run the query in BigQuery
'''
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowException
import re
from airflow.utils.decorators import apply_defaults

# create our operator class with base class as `BaseOperator`
class BigQueryCustomExecuteQueryOperator(BaseOperator):
    template_fields = ['query'] # to replace the jinja templates if present in query string 
    @apply_defaults
    def __init__(
                self,
                gcp_conn_id : str  ,
                sql_file_path : str = None,
                query :str =None , 
                sql_params : dict = {}, 
                use_legacy_sql : bool = False, 
                **kwargs
                ) -> None:
        
        # either sql file path or query should be present
        if not sql_file_path and not query:
            raise AirflowException("Provide either sql_file_path or query")

        super().__init__(**kwargs)        
        self.sql_file_path = sql_file_path
        self.query = query
        self.sql_params = sql_params
        self.use_legacy_sql =use_legacy_sql
        self.gcp_conn_id = gcp_conn_id
    
    # read a file from GCS
    def read_file_from_gcs(self,path : str):
        gcs_hook=GCSHook(gcp_conn_id=self.gcp_conn_id)
        return gcs_hook.download(bucket_name="test_bucket",object_name= path).decode('utf-8')
    
    # method to read sql file and return query
    def read_sql_file(self) -> str:
        with open(self.sql_file_path) as f:
            query = f.read()
        return query
    
    # update the params in the query
    def update_query_params(self , query : str) -> str:
        for param in re.findall(r'\{.*?\}', query):
            param = re.sub(r'[\{\}]','',param)
            query = query.replace(f'{{{param}}}', self.sql_params[param])

        return query

    # execute the query --> main method. Method name should be `execute` only
    def execute(self , context) -> str:
        query=""
        # get the sql file path or query
        if self.sql_file_path:
            query = self.read_sql_file()
        else:
            query = self.query

        # replace params 
        sql_query = self.update_query_params(query) 
        print(sql_query)       

        # use bigquery hook to run the query
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            use_legacy_sql = self.use_legacy_sql,
        )
        result = hook.run_query(sql_query)
        
        return "Query executed successfully"




