# Use Python 3.11 slim as base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive \
    PORT=10000 \
    SHEET_ID=1srvBC83XVx1LS4d8gIiwkWM41sS0Yu3puOHmzwlixrY \
    APIFY_TOKEN=apify_api_XF8XWq7MpAjKR1Yj4TKPBVhqTBGAVj2gHL0D \
    LINKEDIN_USERNAME=kvtvpxgaming@gmail.com \
    LINKEDIN_PASSWORD=Hello@1055

# Install system dependencies including Chrome
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set Chrome environment variables
ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Create and set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose port
EXPOSE 10000

# Start command using python3 app.py instead of gunicorn
CMD ["python3", "app.py"]
