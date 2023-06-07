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
  start_date=pendulum.datetime(2023, 3, 25),
  catchup=False,
)

def podcast_summary():
    @task()
    def get_episodes():
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

      ## Saving extract
      extract.export('./episodes/extract.mp3', format="mp3")
      return '**OK**'

    podcast_episodes = get_episodes()

summary = podcast_summary()
