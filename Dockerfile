# Generated by https://smithery.ai. See: https://smithery.ai/docs/config#dockerfile
FROM python:3.10-slim

# Set work directory
WORKDIR /app

# Install system dependencies if any (optional, adjust if needed)

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire repository
COPY . .

# Expose port if needed (depending on how your server is accessed)
# EXPOSE 8000

# Set the default command to start the MCP server
CMD ["python3", "src/prometheus_mcp_server/server.py"]
