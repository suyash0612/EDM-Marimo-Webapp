# ---------- Base image ----------
FROM python:3.11-bookworm

# Prevent interactive prompts during build
ENV DEBIAN_FRONTEND=noninteractive

# ---------- Install ODBC dependencies ----------
RUN apt-get update && \
    apt-get install -y curl gnupg2 apt-transport-https unixodbc-dev && \
    # Import Microsoft signing key
    curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft.gpg && \
    # Add Microsoft repository for Debian 12 (bookworm)
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*

# ---------- Working directory ----------
WORKDIR /app
COPY . /app

# ---------- Python dependencies ----------
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Networking ----------
EXPOSE 8080

# ---------- Launch ----------
CMD ["marimo", "run", "prj1.py", "--host", "0.0.0.0", "--port", "8080"]
