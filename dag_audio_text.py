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
        mp3 = librosa.util.example_audio_file(filepath)
        y1, sr1 = librosa.load(mp3, sr=16000)
        print(sr1)
        
        print(f"Обработка завершена. **OK**")

    podcast_episodes = resize_audio()

summary = audio_to_text()
