from pyflink.common import WatermarkStrategy, Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor

from pyflink.table.types import DataTypes # added line
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col

import logging

from pyflink.table.udf import udf

"""
This is an example of stream processing computation in Apache Flink. 
The envtionemnt takes a sequence of integers from 1 to 100.
The user defiend function 'map' takes a single value of this sequence as input and returns value + 2.
The value is printed and can be read in the user interface of Apache Flink.
"""


class MyMapFunction(MapFunction):

    def map(self, value):
        return (value, value + 2)


def state_access_demo():
    

    #1. Create streamexecutionenvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    t_env = StreamTableEnvironment.create(env)

    #2. Create data source
    seq_num_source = NumberSequenceSource(1, 100)
    ds = env.from_source(
        source=seq_num_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name='seq_num_source',
        type_info=Types.LONG())

    ds.map(MyMapFunction())

    ds.print()

    #5. Perform the operation
    env.execute()


if __name__ == '__main__':
    state_access_demo()