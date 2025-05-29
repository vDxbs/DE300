FROM python:3.11


# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Java 11 and other dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir numpy pandas pyspark



WORKDIR /app
COPY hw3.ipynb .
CMD ["python", "hw3.ipynb"]