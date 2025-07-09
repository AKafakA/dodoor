# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the subdirectory containing your scripts into the container at /app
COPY deploy/python/function_bench/* ./
COPY deploy/resources/data/workload_data ./deploy/resources/data/workload_data
# If you have a requirements.txt file, uncomment the following lines
COPY deploy/python/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Command to run one of your scripts by default (optional)
# CMD ["python", "./your_main_script.py"]