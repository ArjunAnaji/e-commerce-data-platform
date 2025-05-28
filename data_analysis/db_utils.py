# db_utils.py
import json
from sqlalchemy import create_engine

def get_db_engine(config_path='variables.json'):
    with open(config_path) as f:
        config = json.load(f)

    pg_config = config["postgres_conn"]
    pg_uri = f"postgresql://{pg_config['user']}:{pg_config['password']}@localhost:5432/{pg_config['database']}"
    print(pg_uri)
    pg_engine = create_engine(pg_uri, connect_args={"options": "-csearch_path=data_mart"})

    return pg_engine
