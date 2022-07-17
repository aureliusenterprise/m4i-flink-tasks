from pyflink.common import Row
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.types import DataTypes
from pyflink.table.udf import udf

"""
This is an example of batch processing computation in Apache Flink. 
The table envirionment takes a table as input having an id and data as input columns.
The user defined function 'func1' takes a single row of this table as input and returns the id as-is, and the data doubled: 'Hi' -> 'HiHi'
The output can be read in the User Interface of Apache Atlas.
"""

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements(elements = [(1, 'Hi'), (2, 'Hello')], schema =['id', 'data'])

@udf(result_type=DataTypes.ROW([DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("data", DataTypes.STRING())]))
def func1(input_row: Row) -> Row:
    return Row(input_row.id, input_row.data * 2)

table.map(func1).execute().print()


