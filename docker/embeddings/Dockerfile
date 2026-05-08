# Base image: official AWS EMR Serverless Spark runtime
FROM public.ecr.aws/emr-serverless/spark/emr-7.1.0:latest

# Use root only for installing dependencies, copying project files, and caching the model
USER root

# Install Python dependencies required by the embeddings job
COPY requirements-embeddings.txt /tmp/requirements-embeddings.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements-embeddings.txt

# Store project code outside /app because EMR Serverless may use /app internally
WORKDIR /opt/recommender

# Copy project source code into the custom image
COPY src/ /opt/recommender/src/

# Ensure Python can import project modules
ENV PYTHONPATH=/opt/recommender
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Store Hugging Face and SentenceTransformers cache inside the image
ENV HF_HOME=/opt/recommender/models/huggingface
ENV TRANSFORMERS_CACHE=/opt/recommender/models/huggingface
ENV SENTENCE_TRANSFORMERS_HOME=/opt/recommender/models/sentence-transformers

# Download and save the embedding model during image build
RUN python3 - <<'PY'
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
model.save("/opt/recommender/models/all-MiniLM-L6-v2")
PY

# Ensure the EMR Serverless runtime user can read the project files and cached model
RUN chown -R hadoop:hadoop /opt/recommender

# Run EMR Serverless jobs as the expected non-root user
USER hadoop:hadoop