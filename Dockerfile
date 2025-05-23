FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy requirements.txt first to leverage Docker cache
COPY requirements.txt .

# Update pip and install dependencies
RUN pip install --upgrade pip && \
    grep -v "^logging$" requirements.txt > requirements_fixed.txt && \
    pip install --no-cache-dir -r requirements_fixed.txt

# Copy the rest of the application code
COPY . .

# Expose the port FastAPI runs on
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
