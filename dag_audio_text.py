import xmltodict
import requests
import os
import json

from airflow.decorators import dag, task
import pendulum

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from pydub import AudioSegment

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"

@dag(
  dag_id='podcast',
  schedule="@daily",
  start_date=pendulum.datetime(2023, 3, 25),
  catchup=False,
)

def podcast_summary():
    @task()
    def get_episodes():
        data = requests.get(PODCAST_URL)
        feed = xmltodict.parse(data.text)

        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes. @task get_episodes **OK**")
        return episodes

    podcast_episodes = get_episodes()

summary = podcast_summary()
