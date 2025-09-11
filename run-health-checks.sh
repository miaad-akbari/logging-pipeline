#!/bin/bash

# Health check script
echo "Running health checks..."

# Check if all containers are running
if docker ps | grep -q "Exited"; then
    echo "ERROR: Some containers are not running"
    docker ps -a
    exit 1
fi

# Test MySQL connectivity
docker exec mysql mysql -uroot -prootpassword -e "SHOW DATABASES;" || {
    echo "ERROR: MySQL not responding"
    exit 1
}

# Test Kafka connectivity
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 || {
    echo "ERROR: Kafka not responding"
    exit 1
}

# Test PostgreSQL connectivity
docker exec postgres psql -U user -d mysqllogs -c "SELECT version();" || {
    echo "ERROR: PostgreSQL not responding"
    exit 1
}

# Test consumer connectivity
docker exec consumer python -c "
import psycopg2
try:
    conn = psycopg2.connect(
        dbname='mysqllogs',
        user='user',
        password='password',
        host='postgres',
        connect_timeout=3
    )
    print('✓ PostgreSQL connection successful')
except Exception as e:
    print(f'✗ PostgreSQL connection failed: {e}')
    exit(1)
"

echo "All health checks passed! "