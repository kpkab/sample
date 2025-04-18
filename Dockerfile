# Dockerfile
FROM python:3.10-slim

WORKDIR /code

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app directory
COPY ./app /code/app

# Set the Python path to include the code directory
ENV PYTHONPATH=/code

# Run the application with reload for development
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]