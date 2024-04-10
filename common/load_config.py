import json

def load_kafka_config(config_file):
    with open(config_file, 'r') as file:
        return json.load(file)
