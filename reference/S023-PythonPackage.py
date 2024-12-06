# Databricks notebook source
# library used to parse json data, available in pypi index 
# ModuleNotFoundError: No module named 'simplejson' while importing.. since databrics doesn't have this package installed
# same with your own libraries you create using egg/whl files
import simplejson

# COMMAND ----------

# how to install libraries
# go to compute cluster, select the cluster you work with
# Libraries tab, Install button
# PyPi tab, either mention pacakge name like simplejson or simplejson == version number
# click install , wait installation complete..
# upload s3/dbfs or drag drop jars, egg, wheel files there

# COMMAND ----------

import simplejson

# COMMAND ----------

