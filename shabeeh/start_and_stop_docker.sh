#!/bin/bash

# Navigate to the script's directory
cd "$(dirname "$0")"

# Start the container
docker-compose up -d

# Wait for the process to complete (e.g., 1 hour or however long you expect the task to take)
sleep 3600  # Adjust time as needed

# Stop the container
docker-compose down
