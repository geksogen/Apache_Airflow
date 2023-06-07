## Data Integration (Apache Airflow + Python)

### Apache Airflow install

```bash
apt-get update --fix-missing
apt install python3-pip
apt install ffmpeg
export AIRFLOW_HOME=~/airflow
pip3 install "apache-airflow==2.6.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.1/constraints-3.10.txt"
airflow standalone
```
### Install Python packages

```bash
pip3 install pandas
pip3 install db-sqlite3
pip3 install xmltodict
pip3 install requests
pip3 install 'apache-airflow[pandas]'
pip install apache-airflow[cncf.kubernetes]
pip install python-multipart
pip install aiofiles
pip install asyncio
pip install pydub
pip install vosk
pip install ffmpeg
pip install transformers
pip install ffprobe
pip install torch==1.11.0
mkdir /root/airflow/dags
mkdir /root/airflow/dags/episodes
```

### Create DAG file (get episodes) first task
```Python
import xmltodict
import requests

from airflow.decorators import dag, task
import pendulum

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"

@dag(
  dag_id='podcast_summary',
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
```

### Create Data Base SQLite
```bash
cd ~/
apt install sqlite3
sqlite3 episodes.db
sqlite> .databases                    #DB for store data
sqlite> .quit
airflow connections add 'podcasts' --conn-type 'sqlite' --conn-host 'episodes.db'
airflow connections get podcasts      #Connections details

```

### Create DAG file (create table) second task
```Python
import xmltodict
import requests

from airflow.decorators import dag, task
import pendulum

from airflow.providers.sqlite.operators.sqlite import SqliteOperator


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

summary = podcast_summary()
```
### Create DAG file (storing data in SQLite BD) second task
```Python
import xmltodict
import requests

from airflow.decorators import dag, task
import pendulum

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


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


summary = podcast_summary()
```
### Create DAG file (download podkast file) Fourth task
```Python
import xmltodict
import requests
import os
import json

from airflow.decorators import dag, task
import pendulum

from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"
EPISODE_FOLDER = "~/episodes"

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
            audio_path = os.path.join(./episodes, filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                audio = requests.get(episode["enclosure"]["@url"])
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)

    download_episodes(podcast_episodes)

summary = podcast_summary()
```

```Python
feed.keys()
dict_keys(['rss'])
feed['rss'].keys()
dict_keys(['@version', '@xmlns:itunes', '@xmlns:content', 'channel'])
feed['rss']['channel'].keys()
dict_keys(['title', 'link', 'language', 'copyright', 'description', 'itunes:summary', 'itunes:owner', 'itunes:new-feed-url', 'itunes:image', 'itunes:type', 'itunes:category', 'itunes:author', 'lastBuildDate', 'itunes:explicit', 'generator', 'site', 'item'])
feed['rss']['channel']['item']
```

```bash
scp .\main.py root@212.57.126.174:~/airflow/dags/podcast_summary.py
curl -L https://raw.githubusercontent.com/geksogen/Apache_Airflow/main/main.py > podcast_summary.py
```

### TD;TL

* You could extend this to automatically transcribe and summarize podcasts
* You could get podcasts from more places
* You could automatically create an html page with all of your podcasts and summaries in one place
