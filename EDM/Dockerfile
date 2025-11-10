# Use full Python 3.11 Debian image
FROM python:3.11-bookworm

# Avoid interactive prompts during apt install
ENV DEBIAN_FRONTEND=noninteractive

# ---------- Install system dependencies ----------
RUN apt-get update && \
    apt-get install -y curl gnupg2 apt-transport-https build-essential g++ unixodbc-dev && \
    curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*

# ---------- Set working directory ----------
WORKDIR /app
COPY requirements.txt /app/

# ---------- Install Python dependencies ----------
RUN pip install --upgrade pip wheel setuptools
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Copy project files ----------
COPY . /app

# ---------- Expose Marimo port ----------
EXPOSE 8080

# ---------- Launch Marimo ----------
CMD ["marimo", "run", "prj1.py", "--host", "0.0.0.0", "--port", "8080"]
