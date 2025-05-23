#!/bin/bash

# Create the secrets directory if it doesn't exist
mkdir -p /etc/secrets

# Write the Google credentials to the correct location
echo "$GOOGLE_CREDENTIALS" > /etc/secrets/google-credentials.json

# Start the application
exec gunicorn app:app --bind 0.0.0.0:$PORT 