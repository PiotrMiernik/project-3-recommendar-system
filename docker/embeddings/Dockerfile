# 1. Base: Official AWS EMR Serverless image for Spark
FROM public.ecr.aws/emr-serverless/spark/emr-7.1.0:latest

# 2. Switch to root to perform software installations
USER root

# 3. Set working directory
WORKDIR /app

# 4. Copy requirements file from the root of your project
COPY requirements-embeddings.txt /tmp/requirements-embeddings.txt

# 5. Install Python libraries using the EMR Python interpreter
# --no-cache-dir reduces image size by not storing the installer files
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir -r /tmp/requirements-embeddings.txt

# 6. Set environment variables to ensure Spark uses the correct Python paths
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# 7. Copy ONLY the necessary Python script and common utilities
# We copy src/ to ensure any shared modules in src/common are available
COPY src/ /app/src/

# 8. Switch back to non-root user 'hadoop' (required by EMR Serverless)
USER hadoop