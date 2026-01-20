# Use the official Python image from the Docker Hub as the base image
FROM python:3.13.11-slim

# Copy the 'uv' binary from the specified image into the container
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set the working directory inside the container
WORKDIR /app

# Copy the dependency files into the container
COPY .python-version pyproject.toml uv.lock ./

# Install the dependencies using 'uv' with the locked versions
RUN uv sync --locked


# Copy the pipeline script into the container
COPY pipeline.py pipeline.py

# Command to run the pipeline script when the container starts
ENTRYPOINT ["uv", "run", "python", "pipeline.py"]    