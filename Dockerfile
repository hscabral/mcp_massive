FROM python:3.13-slim

WORKDIR /app

# Install uv for dependency management
RUN pip install uv

COPY . ./

RUN uv pip install --system -e .

ENV PYTHONPATH=/app/src:$PYTHONPATH

# Expose the API port
EXPOSE 8000

# Run the FastAPI server by default
CMD ["uvicorn", "mcp_massive.api:app", "--host", "0.0.0.0", "--port", "8000"]
