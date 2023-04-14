FROM apache/airflow:2.5.3

# Installs PiPy packages specified on 
# requirements.txt on Airflow components
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt