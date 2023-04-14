from datetime import timedelta
from typing import List

from airflow import DAG
from airflow.models.variable import Variable
from airflow.utils.dates import days_ago, parse_execution_date
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator


def get_weather_data(mode: str = "hourly") -> List[dict]:
    """Calls WeatherAPI's API and extracts relevant data
    from response.

    Args:
        mode (str): Either 'hourly' or 'daily'. If 'hourly'
        is passed, the function will return only the
        data point corresponding to the DAG's logical
        timestamp. 'daily' makes it return all data points
        returned by the API.

    Raises:
        Exception: If the API reponse's status code is not
        200.

    Returns:
        List[dict]: List of dictionaries, each corresponding
        to a data point.
    """
    import requests
    import json

    context = get_current_context()
    ds = context["ds"]
    ts_truncated = parse_execution_date(context["ts"]).strftime("%Y-%m-%d %H:%M")
    api_key = Variable.get("secret_weatherapi_key")

    url = "https://api.weatherapi.com/v1/history.json"
    x = requests.get(url, params={"key": api_key, "q": "London", "dt": ds})

    if x.status_code == 200:

        response_dict = json.loads(x.text)
        hour_dicts = response_dict["forecast"]["forecastday"][0]["hour"]
        if mode == "hourly":
            for i in hour_dicts:
                if i["time"] == ts_truncated:
                    selected_hour_dicts = [i]
                    break
        else:
            selected_hour_dicts = hour_dicts

        filtered_dicts = [
            {
                "location": response_dict["location"]["name"],
                "time": hour_dict["time"],
                "temp_celsius": hour_dict["temp_c"],
                "condition": hour_dict["condition"]["text"],
            }
            for hour_dict in selected_hour_dicts
        ]

        return filtered_dicts

    else:
        raise Exception(
            "Response status code: ", x.status_code, "Response text: ", x.text
        )


dag_configs = [
    {"schedule": "hourly", "start_date": days_ago(1), "end_date": None},
    {"schedule": "daily", "start_date": days_ago(10), "end_date": days_ago(2)},
]

for cfg in dag_configs:

    dag_id = f"get_weather_data_{cfg['schedule']}"

    dag = DAG(
        dag_id,
        default_args={
            "retries": 3,
            "retry_delay": timedelta(seconds=30),
            "retry_exponential_backoff": True,
        },
        description=f"Retrieves weather data from WeatherAPI on a {cfg['schedule']} basis.",
        schedule=f"@{cfg['schedule']}",
        start_date=cfg["start_date"],
        end_date=cfg["end_date"],
        max_active_runs=3,
        user_defined_filters={
            "to_records": lambda x: [str(tuple(i.values())) for i in x]
        },
    )

    get_weather_data_task = PythonOperator(
        task_id="get_weather_data",
        python_callable=get_weather_data,
        dag=dag,
        op_kwargs={"mode": cfg["schedule"]},
    )

    pg_insert_task = PostgresOperator(
        task_id="insert_data_to_table",
        postgres_conn_id="data_warehouse_connection",
        sql="sql/insert.sql",
        dag=dag,
    )

    get_weather_data_task.set_downstream(pg_insert_task)

    globals()[dag_id] = dag
