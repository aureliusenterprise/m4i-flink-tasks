################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import (FileSource, StreamFormat, FileSink,
                                                       OutputFileConfig, RollingPolicy)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.functions import ProcessFunction, RuntimeContext
from pyflink.datastream.functions import MapFunction

output_tag = OutputTag("side-output", Types.STRING())

class MyProcessFunction(ProcessFunction):

    def process_element(self, value: int, ctx: ProcessFunction.Context):
        # emit data to regular output
        yield value

        # emit data to side output
        yield output_tag, "sideout-" + str(value)

def split(line):
    yield from line.split()
    
    

data = [1,2,3,4,5,6,7]

if __name__ == '__main__':
    #logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_buffer_timeout(0)
    #env.set_runtime_mode(RuntimeExecutionMode.STREAM)
    # write all the data to one file
    env.set_parallelism(1)
    
    logging.error("start processing....")
    ds = env.from_collection(data)
    
    main_data_stream = ds.process(MyProcessFunction(), Types.INT()).name('process function')
    main_data_stream.print()
    
    side_output_stream = main_data_stream.get_side_output(output_tag)
    side_output_stream.print()

    # submit for execution
    env.execute_async("sideoutput_example")
