#!/bin/sh
# entrypoint.sh

# Example usage: set APP_SCRIPT=producer1.py or APP_SCRIPT=consumer1.py at runtime
exec python "$APP_SCRIPT"
