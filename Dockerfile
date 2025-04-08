FROM python:3.10

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create static directory for Swagger
RUN mkdir -p /app/static && chmod 777 /app/static

# Copy all application code and files
COPY . .

# Copy Swagger files to static directory (if not already in project root)
COPY swagger.yaml /app/static/

# Set default command to run your app
CMD ["python", "run.py"]
