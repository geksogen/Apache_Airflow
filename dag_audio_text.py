import xmltodict
import requests
import os
import json

from airflow.decorators import dag, task
import pendulum

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from pydub import AudioSegment

@dag(
  dag_id='podcast_audio_text',
  schedule="@daily",
  start_date=pendulum.datetime(2023, 5, 25),
  catchup=False,
)

def podcast_summary():
    @task()
    def get_episodes():
        data = 'test'
        return data

    test = get_episodes()
    print(test)

summary = podcast_summary()
print("hello")
