# DOTA2 REDDIT API PIPELINE PROJECT

## Setup

Steps:
| Description | Window | Linux |
| ----------------------------------------- | ---------------------------------- | ------------------------------------ |
| 1. Create docker compose file for airflow | `Invoke-WebRequest -Uri 'https://airflow.apache.org/docs/apache-airflow/2.3.2/docker-compose.yaml' -OutFile 'docker-compose.yaml'` | `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.3.2/docker-compose.yaml'` |
| 2. Create virtual environment (optionals) | `python -m venv venv` | `virtualenv venv` |
| 3. Use virtual environment (optionals) | `.\venv\Scripts\Activate` | `source venv/bin/activate` |
| 4. Install dependencies | `pip install -r requirements.txt` | `pip install -r requirements.txt` |

## Tips

**How to deactivate venv run the following command** <br>
`deactivate` <br>
**How to delete venv folder**
|Window|Linux|
| ----------------- | ----------------- |
| `Remove-Item -Recurse -Force venv` | `rm -rf venv` |
