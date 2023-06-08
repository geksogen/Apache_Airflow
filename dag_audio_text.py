import xmltodict
import requests
import os
import json

from airflow.decorators import dag, task
import pendulum

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from pydub import AudioSegment
from vosk import Model, KaldiRecognizer

import librosa

@dag(
  dag_id='podcast',
  schedule="@daily",
  start_date=pendulum.datetime(2023, 3, 25),
  catchup=False,
)

def audio_to_text():
    @task()
    def resize_audio():
        
        FRAME_RATE = 16000
        
        filepath = os.path.join('./episodes', 'sound.mp3')
        mp3 = AudioSegment.from_mp3(filepath)
        mp3 = mp3.set_channels(1)
        mp3 = mp3.set_frame_rate(FRAME_RATE)
        
        print(f"Обработка завершена. **OK**")

    podcast_episodes = resize_audio()

summary = audio_to_text()
