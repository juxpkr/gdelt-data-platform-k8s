#!/bin/bash
pkill -f "kubectl port-forward" 2>/dev/null
sleep 1

kubectl port-forward --address 0.0.0.0 -n gdelt svc/airflow-api-server 8080:8080 &
kubectl port-forward --address 0.0.0.0 -n gdelt svc/minio 9002:9001 &
kubectl port-forward --address 0.0.0.0 -n gdelt svc/grafana 3000:3000 &

echo "Airflow:  http://100.78.48.65:8080"
echo "MinIO:    http://100.78.48.65:9002"
echo "Grafana:  http://100.78.48.65:3000"
echo ""
echo "Ctrl+C to stop all"
wait
