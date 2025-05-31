from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
import random

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import boto3
import os

# Set up a DAG to train a linear regression model after 20 hours and 40 hours of data are collected:
# Combine the available observations collected so far.
# For each station, predict the temperature for the next 8 hours (in half-hour increments).
# You may include stations as categorical predictors and use any available predictors you have collected.
# Save the model predictions in a separate .csv file and upload to your S3 bucket, under directory predictions.

