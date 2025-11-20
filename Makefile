.PHONY: help cluster-up cluster-down deploy clean status logs-airflow port-forward restart-airflow

# ANSI 색상 코드
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
CYAN := \033[0;36m
NC := \033[0m  # No Color

# 기본 변수
CLUSTER_NAME := gdelt-platform
NAMESPACE := airflow

# 도움말
help:
	@echo "=========================================="
	@echo "GDELT Data Platform - Kubernetes"
	@echo "=========================================="
	@echo ""
	@echo "Available commands:"
	@echo "  make cluster-up      - Create Kind cluster and deploy infrastructure"
	@echo "  make cluster-down    - Delete Kind cluster"
	@echo "  make deploy          - Deploy applications (Airflow, etc.)"
	@echo "  make clean           - Clean all resources"
	@echo "  make status          - Check cluster status"
	@echo "  make logs-airflow    - View Airflow logs"
	@echo "  make port-forward    - Port forward Airflow (8080)"
	@echo "  make restart-airflow - Restart Airflow webserver"
	@echo ""

# 클러스터 생성 및 기본 인프라 배포
cluster-up:
	@echo "$(BLUE)[INFO]$(NC) Creating Kind cluster..."
	kind create cluster --name $(CLUSTER_NAME) --config deploy/overlays/dev-kind/kind-config.yaml
	@echo ""
	@echo "$(BLUE)[INFO]$(NC) Waiting for cluster to be ready..."
	kubectl wait --for=condition=ready nodes --all --timeout=300s
	@echo ""
	@echo "$(BLUE)[INFO]$(NC) Installing MetalLB..."
	kubectl apply -f deploy/base/metallb/metallb-install.yaml
	kubectl wait --namespace metallb-system --for=condition=ready pod --selector=app=metallb --timeout=90s
	kubectl apply -f deploy/overlays/dev-kind/metallb-config.yaml
	@echo ""
	@echo "$(BLUE)[INFO]$(NC) Installing Ingress-Nginx..."
	kubectl apply -f deploy/base/ingress/ingress-nginx.yaml
	kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=90s
	@echo ""
	@echo "$(GREEN)[SUCCESS]$(NC) Cluster is ready!"
	@echo ""
	@echo "Next step: make deploy"

# 클러스터 삭제
cluster-down:
	@echo "$(YELLOW)[INFO]$(NC) Deleting Kind cluster..."
	kind delete cluster --name $(CLUSTER_NAME)
	@echo "$(GREEN)[SUCCESS]$(NC) Cluster deleted successfully"

# Airflow 등 애플리케이션 배포
deploy:
	@echo "$(BLUE)[INFO]$(NC) Creating namespace..."
	kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	@echo ""
	@echo "$(BLUE)[INFO]$(NC) Deploying PostgreSQL..."
	kubectl apply -f deploy/base/databases/postgres/external-service.yaml
	@echo ""
	@echo "$(BLUE)[INFO]$(NC) Deploying Airflow via Helm..."
	helm repo add apache-airflow https://airflow.apache.org
	helm repo update
	helm upgrade --install airflow apache-airflow/airflow \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--version 1.14.0 \
		-f deploy/base/orchestration/airflow/values.yaml \
		--debug
	@echo ""
	@echo "$(GREEN)[SUCCESS]$(NC) Airflow deployment triggered!"
	@echo "Check status with: make status"

# 전체 정리
clean: cluster-down
	@echo "$(YELLOW)[INFO]$(NC) Cleaning up resources..."
	@echo "$(GREEN)[SUCCESS]$(NC) All resources cleaned"

# 클러스터 상태 확인
status:
	@echo "$(CYAN)[STATUS]$(NC) Cluster Status:"
	@echo ""
	@echo "=== Nodes ==="
	@kubectl get nodes
	@echo ""
	@echo "=== All Pods ==="
	@kubectl get pods -A
	@echo ""
	@echo "=== Services ==="
	@kubectl get svc -A

# Airflow 로그 확인
logs-airflow:
	@echo "$(CYAN)[LOGS]$(NC) Airflow Webserver Logs:"
	@kubectl logs -n $(NAMESPACE) -l component=webserver --tail=100 -f

# 포트 포워딩
port-forward:
	@echo "$(BLUE)[INFO]$(NC) Port forwarding Airflow webserver to localhost:8080..."
	@echo "$(CYAN)[INFO]$(NC) Airflow UI: http://localhost:8080"
	@kubectl port-forward -n $(NAMESPACE) svc/airflow-webserver 8080:8080

# Airflow 재시작 (개발 시 유용)
restart-airflow:
	@echo "$(YELLOW)[INFO]$(NC) Restarting Airflow webserver..."
	@kubectl rollout restart -n $(NAMESPACE) deployment/airflow-webserver
	@echo "$(GREEN)[SUCCESS]$(NC) Airflow restarted"
	@echo "Check status: make status"
