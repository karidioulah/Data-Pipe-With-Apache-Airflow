from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = "",
                 table = "",
                 query = "",
                 staging_events_table="",
                 staging_songs_table="",
                 page="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.staging_events_table = staging_events_table
        self.staging_songs_table = staging_songs_table
        self.page = page
        
        
    def execute(self, context):
        self.log.info('starting fact data load')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        insert_songplay = self.query.format(
            self.table,
            self.staging_events_table,
            self.page ,
            self.staging_songs_table
        )
        
        self.log.info(insert_songplay)
        redshift.run(insert_songplay)
        
        

