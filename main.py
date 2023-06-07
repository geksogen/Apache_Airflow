import xmltodict
import requests
import os
import json

from airflow.decorators import dag, task
import pendulum

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

import aiofiles
from vosk import Model, KaldiRecognizer, SetLogLevel
from pydub import AudioSegment
import subprocess

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"

@dag(
  dag_id='podcast_summary',
  schedule="@daily",
  start_date=pendulum.datetime(2023, 3, 25),
  catchup=False,
)

def podcast_summary():
    create_database = SqliteOperator(
        task_id='create_table_sqlite',
        sql=r"""
            CREATE TABLE IF NOT EXISTS episodes (
                link TEXT PRIMARY KEY,
                title TEXT,
                filename TEXT,
                published TEXT,
                description TEXT,
                transcript TEXT
            );
            """,
        sqlite_conn_id="podcasts"
    )

    @task()
    def get_episodes():
        data = requests.get(PODCAST_URL)
        feed = xmltodict.parse(data.text)

        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes. @task get_episodes **OK**")
        return episodes

    podcast_episodes = get_episodes()
    create_database.set_downstream(podcast_episodes)

    @task()
    def load_episodes(episodes):
        hook = SqliteHook(sqlite_conn_id="podcasts")
        stored_episodes = hook.get_pandas_df("SELECT * from episodes;")
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored_episodes["link"].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append(
                    [episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])

        hook.insert_rows(table='episodes', rows=new_episodes,
                         target_fields=["link", "title", "published", "description", "filename"])

    load_episodes(podcast_episodes)

    @task()
    def download_episodes(episodes):
        for episode in episodes:
            filename = f"{episode['link'].split('/')[-1]}.mp3"
            audio_path = os.path.join('./episodes', filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                audio = requests.get(episode["enclosure"]["@url"])
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)

    download_episodes(podcast_episodes)
    
    @task()
    def audio_text(episodes):
      #extract
      startMin = 0
      startSec = 0
      endMin = 0
      endSec = 60
      # Time to miliseconds
      startTime = startMin * 60 * 1000 + startSec * 1000
      endTime = endMin * 60 * 1000 + endSec * 1000

      # Opening file and extracting segment
      song = AudioSegment.from_mp3('./episodes/soung.mp3')
      extract = song[startTime:endTime]

      # Saving extract
      extract.export('./episodes/extract.mp3', format="mp3")
    
    audio_text(podcast_episodes)
summary = podcast_summary()
