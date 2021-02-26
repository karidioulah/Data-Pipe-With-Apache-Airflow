from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 query = "",
                 insert_mode = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Maping parameters
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.insert_mode = insert_mode
        
    def execute(self, context):
        self.log.info('Starting Load Dimension table')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        delete_from_table = "DELETE FROM {}".format(self.table)
        
        load_dim_table_statement = self.query.format(self.table)
        
        self.log.info(load_dim_table_statement)
        if self.insert_mode == 'empty_table':
            redshift.run(delete_from_table)
            
        redshift.run(load_dim_table_statement)
        
