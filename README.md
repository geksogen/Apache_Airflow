## Data Integration (Apache Airflow + Python)

### Apache Airflow install

```bash
apt install python3-pip
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
mkdir /root/airflow/dags
```