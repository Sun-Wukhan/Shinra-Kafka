#!/bin/bash

pip3 install -r requirements.txt

python3 producers/energy_distribution_grid_producer.py &
python3 producers/environmental_impact_reports_producer.py &  # Fixed typo in the filename
python3 producers/mako_production_live_producer.py &
python3 producers/security_alerts_producer.py &
python3 producers/shinra_news_update_producer.py &

wait
