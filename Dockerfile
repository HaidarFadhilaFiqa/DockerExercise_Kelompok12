From python:latest
WORKDIR /ingestion
COPY data_ingestion_script.py .
COPY requirements.txt .
RUN pip install -r requirements.txt
CMD ["python", "./ingestion/data_ingestion_script.py"]
