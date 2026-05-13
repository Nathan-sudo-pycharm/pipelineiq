# Start from the official Python 3.11 slim image
# "slim" means it's a minimal version — smaller image size
# which means faster downloads and less attack surface in production
FROM python:3.11-slim

# Set the working directory inside the container
# All subsequent commands run from this folder
WORKDIR /app

# Copy requirements first — before copying the rest of the code
# This is a Docker best practice called layer caching
# If your code changes but requirements don't, Docker skips
# reinstalling dependencies and reuses the cached layer
# This makes rebuilds much faster
COPY requirements.txt .

# Install all dependencies
# --no-cache-dir means don't store the pip cache inside the image
# keeping the image size smaller
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the rest of the app code into the container
COPY app/ .

# Expose port 8000 so Docker knows this container listens on it
EXPOSE 8000

# Default command — starts the FastAPI app
# 0.0.0.0 means "listen on all network interfaces"
# without this, the app would only be reachable from inside the container
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]