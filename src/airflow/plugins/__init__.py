from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_dend_capstone_plugin"
    operators = [
        operators.StageCsvToPostgresOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]