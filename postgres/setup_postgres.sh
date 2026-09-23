#!/usr/bin/env bash
# L3 VM only. Run from this folder:  bash setup_postgres.sh
set -e
ZT_SUBNET=10.147.17.0/24   # <-- your ZeroTier subnet
sudo apt-get update && sudo apt-get install -y postgresql
sudo -u postgres psql -c "CREATE USER fraud WITH PASSWORD 'fraud';" || true
sudo -u postgres psql -c "CREATE DATABASE fraud OWNER fraud;" || true
PGPASSWORD=fraud psql -h localhost -U fraud -d fraud -f init.sql
CONF=$(sudo -u postgres psql -tAc "SHOW config_file"); HBA=$(sudo -u postgres psql -tAc "SHOW hba_file")
sudo sed -i "s/^#\?listen_addresses.*/listen_addresses = '*'/" "$CONF"
grep -q "fraud fraud $ZT_SUBNET" "$HBA" || echo "host fraud fraud $ZT_SUBNET scram-sha-256" | sudo tee -a "$HBA"
sudo systemctl restart postgresql
if command -v ufw >/dev/null && sudo ufw status | grep -q active; then sudo ufw allow 5432/tcp; fi
echo "Postgres ready. Test from L4:  PGPASSWORD=fraud psql -h <L3-ZT-IP> -U fraud -d fraud -c 'SELECT 1'"
