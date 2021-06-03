from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 #conn_id = redshift,
                 redshift_conn_id="",
                 payment_levels="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.payment_levels=payment_levels
        # Map params here
        # Example:
        #self.conn_id = conn_id
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        query = SqlQueries.data_quality_check_user_level
        result = redshift.get_records(query)
        if result[0][0] != self.payment_levels:
            raise ValueError(f"There are {result[0][0]} payment levels")
            
         