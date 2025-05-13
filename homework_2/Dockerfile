FROM python:3.10
WORKDIR /app
COPY hw2.py .
RUN pip install --no-cache-dir \
    pandas \
    duckdb==1.2.2 \
    cassandra-driver
CMD ["python", "hw2.py"]