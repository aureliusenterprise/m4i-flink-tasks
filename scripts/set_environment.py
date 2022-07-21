from pyflink.datastream import StreamExecutionEnvironment
import os

PYTHON_ENV = "flink_env"

def set_env(env: StreamExecutionEnvironment):
    pass
    # path = os.path.dirname(__file__)
    # env.set_python_executable(
    #     path + f"/{PYTHON_ENV}/bin/python")
    # env.set_python_requirements(
    #     path + "/requirements.txt")
