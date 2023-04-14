INSERT INTO
    weather.hourly
VALUES
    {{ ti.xcom_pull(task_ids = "get_weather_data") | to_records | join(", ") }}