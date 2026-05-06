# Base image: official AWS EMR Serverless Spark runtime
FROM public.ecr.aws/emr-serverless/spark/emr-7.1.0:latest

# Use root only for installing dependencies and copying project files
USER root

# Install Python dependencies required by the embeddings job
COPY requirements-embeddings.txt /tmp/requirements-embeddings.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements-embeddings.txt

# Store project code outside /app because EMR Serverless may use /app internally
WORKDIR /opt/recommender

# Copy project source code into the custom image
COPY src/ /opt/recommender/src/

# Ensure the EMR Serverless runtime user can read the project files
RUN chown -R hadoop:hadoop /opt/recommender

# Ensure Python can import modules from src/
ENV PYTHONPATH=/opt/recommender
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Run EMR Serverless jobs as the expected non-root user
USER hadoop:hadoop