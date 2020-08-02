from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class LoadDimensionOperator(BaseOperator):
    '''
    Load data into a dimension table in Redshift from other Redshift tables.

    Args:
        conn_id: Connection ID for Redshift.
        table_name: Target table where to load the data.
        table_create: SQL statement that creates the table.
        table_select: SQL statement that selects the data to load into the table.
        drop_table: Whether to drop the table before loading the data or not.
        truncate_table: Whether to truncate the table before loading the data
            or not.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.
    '''

    ui_color = '#80BD9E'

    table_truncate = 'TRUNCATE TABLE {table_name} RESTART IDENTITY;'

    table_drop = 'DROP TABLE IF EXISTS {table_name};'

    table_insert = '''
        INSERT INTO {table_name}
        {table_select};
    '''

    @apply_defaults
    def __init__(
        self,
        conn_id,
        table_name,
        table_create,
        table_select,
        drop_table=False,
        truncate_table=False,
        *args,
        **kwargs,
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.table_name = table_name
        self.table_create = table_create
        self.table_select = table_select
        self.drop_table = drop_table
        self.truncate_table = truncate_table

    def execute(self, context):
        postgres_hook = PostgresHook(self.conn_id)

        if self.drop_table:
            self.log.info(f'Dropping table "{self.table_name}"')
            postgres_hook.run(self.table_drop.format(table_name=self.table_name))

        if self.truncate_table:
            self.log.info(f'Truncating table "{self.table_name}"')
            postgres_hook.run(self.table_truncate.format(table_name=self.table_name))

        self.log.info(f'Loading data into "{self.table_name}"')

        postgres_hook.run(self.table_create)
        postgres_hook.run(
            self.table_insert.format(
                table_name=self.table_name, table_select=self.table_select
            )
        )
