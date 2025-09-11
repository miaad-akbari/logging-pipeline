# consumer/Dockerfile

# Using a slim python image for smaller size
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer script
COPY main.py .

# Command to run the script
CMD ["python", "main.py"]