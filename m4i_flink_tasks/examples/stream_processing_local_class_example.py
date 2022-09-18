from pyflink.common import WatermarkStrategy, Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction, FlatMapFunction
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

The approch here has the benefit that it seperates the logic from the stream processing
such that development and unit tests can be build easily and independent of flink.
The general agreement here is that the local map function contains the logic and in case of a 
problem throws an exception which can be catched and handeled by the MyMapFunction.

In pyflink version 1.17 a new concept called side output is introduced, which allows to define 
of a single stream to have multiple output streams. This way it is possible to have an extra 
output stream for the dead letter box can be defined and then the kafka producer can be 
specified on flink level and not in the MapFunction class itself.
"""

class LocalMapFunction(object):
    
    def map_local(self, value):
        return (value, value + 2)
# end of class LocalFlatMapFunction


class MyMapFunction(MapFunction,LocalMapFunction):
    def map(self, value):
        try:
        #wrapper = self.get_wrapper()
            return self.map_local(value)
        except Exception as e:
            logging.error(str(e))
        return
                
def state_access_demo():
    

    #1. Create streamexecutionenvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    
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


def state_access_demo1():
    

    #1. Create streamexecutionenvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    #output_tag = OutputTag("deadletter", Types.STRING())
    env.set_parallelism(1)
    
    #t_env = StreamTableEnvironment.create(env)

    #2. Create data source
    seq_num_source = NumberSequenceSource(1, 100)
    data_stream = env.from_source(
        source=seq_num_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name='seq_num_source',
        type_info=Types.LONG())

    # ds.map(MyMapFunction())

    # ds.print()
    #data_stream = env.add_source(seq_num_source).name("seq_num_source")
    data_stream = data_stream.map(MyMapFunction(), Types.LONG()).name("local operation").filter(lambda notif: notif)
    data_stream.print()

    #5. Perform the operation
    env.execute("example_with_side_output")


if __name__ == '__main__':
    state_access_demo()