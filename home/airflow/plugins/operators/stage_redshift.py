from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials = "",
                 aws_iam_role="",
                 s3_bucket = "",
                 s3_key = "",
                 table = "",
                 query = "",
                 region = "us-west-2",
                 json = "auto",
                 dateformat = "auto",
                 timeformat = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.aws_iam_role = aws_iam_role
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.query = query
        self.region = region
        self.json = json
        self.timeformat = timeformat
        self.dateformat = dateformat

    def execute(self, context):
        self.log.info('Start staging data')
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        self.log.info(redshift)
       
        path = 's3://{}/{}'.format(self.s3_bucket,self.s3_key)
        insert_staging = self.query.format(
            self.table,
            path,
            self.region,
            self.json,
            self.dateformat,
            self.timeformat,
            credentials.access_key,
            credentials.secret_key)
        
        self.log.info(insert_staging)
        
        redshift.run(insert_staging)
            
            
        
        
             
            
            
        
        





