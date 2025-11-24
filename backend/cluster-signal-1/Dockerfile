# Use the official lightweight Python image
FROM python:3.11-slim

# Allow statements and log messages to immediately appear in the Cloud Run logs
ENV PYTHONUNBUFFERED=1

# Set working directory in container
WORKDIR /app

# Copy dependency files
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .

# Create a non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port 8080 (Cloud Run default)
EXPOSE 8080

# Set environment variable for port
ENV PORT=8080

# Run the web service using gunicorn with proper configuration
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app