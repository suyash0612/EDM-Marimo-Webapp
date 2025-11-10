# Use Python base image
FROM python:3.11-slim

# Install system dependencies for pyodbc + SQL Server
RUN apt-get update && \
    apt-get install -y curl gnupg unixodbc-dev && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Set working directory and copy project files
WORKDIR /app
COPY . /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port for Marimo
EXPOSE 8080

# Run Marimo webapp
CMD ["marimo", "run", "prj1.py", "--host", "0.0.0.0", "--port", "8080"]
