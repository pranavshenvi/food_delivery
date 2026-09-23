#!/usr/bin/env bash
# Usage on L2/L3 VM only:  bash setup_kafka.sh 2   (3 on L3). L1 and L4 do not run Kafka. Run from this folder.
set -e
N=$1; KV=3.8.1; CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk   # same cluster id on both nodes
[ -z "$N" ] && { echo "usage: bash setup_kafka.sh <2|3>"; exit 1; }
sudo apt-get update && sudo apt-get install -y openjdk-17-jre-headless wget
if [ ! -d /opt/kafka ]; then
  wget -q https://archive.apache.org/dist/kafka/$KV/kafka_2.13-$KV.tgz
  sudo tar -xzf kafka_2.13-$KV.tgz -C /opt && sudo mv /opt/kafka_2.13-$KV /opt/kafka
  sudo chown -R "$USER" /opt/kafka
fi
sudo mkdir -p /opt/kafka-data && sudo chown "$USER" /opt/kafka-data
cp "server-L$N.properties" /opt/kafka/config/kraft/fraud.properties
/opt/kafka/bin/kafka-storage.sh format -t $CLUSTER_ID -c /opt/kafka/config/kraft/fraud.properties --ignore-formatted
if command -v ufw >/dev/null && sudo ufw status | grep -q active; then sudo ufw allow 9092/tcp; sudo ufw allow 9093/tcp; fi
echo "Start with:  KAFKA_HEAP_OPTS='-Xmx512m -Xms512m' /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/fraud.properties"
