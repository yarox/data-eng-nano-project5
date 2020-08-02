from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class StageToPostgresOperator(BaseOperator):
    '''
    Load JSON files from the local filesystem into a staging table in PostgreSQL.

    Args:
        conn_id: Connection ID for PostgreSql.
        table_name: Staging table where to load the data.
        data_path: Path where JSON data files reside.
        table_create: SQL statement that creates the staging table.
        drop_table: Whether to drop the staging table before loading the data
            or not.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.
    '''

    ui_color = '#fc03f4'

    staging_table_json_create = '''
        CREATE TABLE {table_name}_json (
            id   SERIAL PRIMARY KEY,
            data JSONB
        );
    '''

    staging_table_copy = '''
        COPY {table_name}_json(data)
        FROM PROGRAM 'find {data_path} -name *.json -exec cat {{}} \; -exec echo \;'
        CSV QUOTE e'\x01' DELIMITER e'\x02';
    '''

    staging_table_drop = 'DROP TABLE IF EXISTS {table_name};'

    @apply_defaults
    def __init__(
        self,
        conn_id,
        table_name,
        data_path,
        table_insert,
        drop_table=False,
        *args,
        **kwargs,
    ):
        super(StageToPostgresOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.table_name = table_name
        self.data_path = data_path
        self.table_insert = table_insert
        self.drop_table = drop_table

    def execute(self, context):
        postgres_hook = PostgresHook(self.conn_id)

        if self.drop_table:
            self.log.info(f'Dropping table "{self.table_name}"')
            postgres_hook.run(
                self.staging_table_drop.format(table_name=self.table_name)
            )

        self.log.info(
            f'Loading raw JSON data from "{self.data_path}" into "{self.table_name}_json"'
        )

        postgres_hook.run(
            self.staging_table_json_create.format(table_name=self.table_name)
        )
        postgres_hook.run(
            self.staging_table_copy.format(
                table_name=self.table_name, data_path=self.data_path
            )
        )

        self.log.info(
            f'Loading data from "{self.table_name}_json" into "{self.table_name}"'
        )

        postgres_hook.run(self.table_insert)
        postgres_hook.run(
            self.staging_table_drop.format(table_name=self.table_name + '_json')
        )
