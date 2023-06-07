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

def audio_to_text():
    @task()
    def resize_audio():
        data = requests.get(PODCAST_URL)
        feed = xmltodict.parse(data.text)

        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes. **OK**")
        return episodes

    podcast_episodes = resize_audio()

summary = audio_to_text()
