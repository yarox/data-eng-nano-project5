from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    '''
    Load JSON files from S3 into a staging table in Redshift.

    Args:
        conn_id: Connection ID for Redshift.
        credentials_id: Connection ID for AWS credentials.
        table_name: Staging table where to load the data.
        table_create: SQL statement that creates the staging table.
        s3_bucket: S3 bucket where JSON data resides.
        s3_key: Specific path in the S3 bucket where JSON data files reside.
        region: AWS region where the source data is located.
        json_option: How Redshift will load the fields from the JSON files.
            Either 'auto' or a JSONPaths file.
        drop_table: Whether to drop the staging table before loading the data
            or not.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.
    '''

    ui_color = '#358140'

    template_fields = ('s3_key',)

    staging_table_drop = 'DROP TABLE IF EXISTS {table_name};'

    staging_table_copy = '''
        COPY {table_name}
        FROM '{data_path}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        TIMEFORMAT as 'epochmillisecs'
        REGION '{region}'
        FORMAT AS JSON '{json_option}';
    '''

    @apply_defaults
    def __init__(
        self,
        conn_id,
        credentials_id,
        table_name,
        table_create,
        s3_bucket,
        s3_key,
        region,
        json_option,
        drop_table=False,
        *args,
        **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.credentials_id = credentials_id
        self.table_name = table_name
        self.table_create = table_create
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_option = json_option
        self.drop_table = drop_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)

        aws_hook = AwsHook(self.credentials_id)
        credentials = aws_hook.get_credentials()

        if self.drop_table:
            self.log.info(f'Dropping table "{self.table_name}"')
            redshift_hook.run(
                self.staging_table_drop.format(table_name=self.table_name)
            )

        s3_key = self.s3_key.format(**context)
        data_path = f's3://{self.s3_bucket}/{s3_key}'

        self.log.info(
            f'Loading raw JSON data from "{data_path}" into "{self.table_name}"'
        )

        redshift_hook.run(self.table_create)
        redshift_hook.run(
            self.staging_table_copy.format(
                table_name=self.table_name,
                data_path=data_path,
                access_key_id=credentials.access_key,
                secret_access_key=credentials.secret_key,
                region=self.region,
                json_option=self.json_option,
            )
        )
