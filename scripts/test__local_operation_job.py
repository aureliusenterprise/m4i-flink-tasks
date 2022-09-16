# -*- coding: utf-8 -*-

from m4i_flink_tasks.operation import LocalOperationLocal
from m4i_atlas_core import ConfigStore as m4i_ConfigStore

from config import config
from credentials import credentials

m4i_store = m4i_ConfigStore.get_instance()
