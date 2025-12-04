# Use Prefect base image
FROM prefecthq/prefect:3.6.4-python3.14

# Set working directory
WORKDIR /app

# Copy dependencies first for caching
COPY requirements.txt .

# Install all dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose Prefect UI port
EXPOSE 4200

# Default command: start server + run pipeline
CMD bash -c "\
  prefect server start --host 0.0.0.0 --port 4200 & \
  sleep 10 && \
  python -m src.flows.serve_all \
"
