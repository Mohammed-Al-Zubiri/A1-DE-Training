docker exec -it kafka1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic ecommerce_json_events \
  --partitions 3 \
  --replication-factor 3