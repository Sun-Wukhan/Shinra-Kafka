#!/bin/bash

pip3 install -r requirements.txt

python3 consumers/energy_distribution_grid_consumer.py &
python3 consumers/environmental_impact_reports_consumer.py &  # Ensure this file exists
python3 consumers/mako_production_live_consumer.py &
python3 consumers/security_alerts_consumer.py &
python3 consumers/shinra_news_update_consumer.py &

wait
