from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
   
    @apply_defaults
    def __init__(self,
                 # operators params defining 
                 redshift_conn_id = "",
                 list_table = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Maping params 
        self.redshift_conn_id = redshift_conn_id
        self.list_table = list_table
       

    def execute(self, context):
        self.log.info('Checking table data quality')
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for table, column in self.list_table.items():
            check_statement = """SELECT COUNT(*)
                                 FROM {}
                                 WHERE {} IS NULL""".format(table,column)
            self.log.info(check_statement)
        
        
            result = redshift.get_records(check_statement)
        
            self.log.info(result[0][0])
            if result[0][0] != 0 :
                raise ValueError(f"Data quality check failed. {column} returned NULL value")
             
            
            self.log.info(f"Data quality check {table} is okay")
            
            
            
        
    
        