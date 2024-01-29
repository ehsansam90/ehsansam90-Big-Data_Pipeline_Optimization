# Use an official Python runtime as a parent image
FROM docker.io/bitnami/spark:3.5

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY docker_transform_pyspark.py .
COPY requirements.txt .

# Upgrade pip
RUN pip install --upgrade pip
# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir --use-feature=fast-deps -r requirements.txt

# Define environment variable
ENV PYTHONUNBUFFERED 1

# Run your_script.py when the container launches
CMD ["spark-submit", "docker_transform.py"]

