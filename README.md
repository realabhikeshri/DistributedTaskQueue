# DistributedTaskQueue

## 1. Project Title

DistributedTaskQueue

A production-grade distributed task queue built with .NET 9 and Redis. This project implements a horizontally scalable, fault-tolerant background processing system with visibility timeouts, retry policies, circuit breaking, rate limiting, idempotency safeguards, metrics, Kubernetes deployment support, and load testing capabilities.

---

## 2. Problem Statement

Modern distributed systems rely heavily on background processing for tasks such as notifications, payments, data processing, and integrations. These systems must handle:

- Worker crashes
- Network failures
- Retry storms
- Concurrency control
- Backpressure management
- Horizontal scaling
- Observability and monitoring

DistributedTaskQueue models a real-world background processing system with strong reliability guarantees, controlled concurrency, and production-ready observability.

---

## 3. Architecture Overview

The system is organized into clean architectural boundaries:

DistributedTaskQueue  
│  
├── src  
│   ├── DistributedTaskQueue.Core  
│   ├── DistributedTaskQueue.Infrastructure  
│   ├── DistributedTaskQueue.Producer  
│   ├── DistributedTaskQueue.Worker  
│   └── DistributedTaskQueue.LoadTest  
│  
├── k8s  
├── monitoring  
└── DistributedTaskQueue.sln  

Core principles:

- Clean separation of concerns
- Infrastructure isolated from domain logic
- Atomic queue state transitions using Redis Lua scripts
- Horizontal scalability via stateless workers
- Observability-first design

System Flow:

1. Enqueue  
   Tasks are serialized and pushed into Redis lists. Weighted queues allow prioritization.

2. Atomic Dequeue  
   A Lua script:
   - Pops from main queue  
   - Adds to processing set  
   - Adds to visibility timeout ZSET  
   - Ensures atomic transition  

   This prevents race conditions between workers.

3. Execution Pipeline  
   Each task passes through:
   - Circuit breaker check  
   - Rate limiter validation  
   - Idempotency lock acquisition  
   - Task handler execution  
   - Success marking or retry scheduling  

4. Visibility Timeout Monitor  
   - Detects expired tasks in ZSET  
   - Re-enqueues orphaned tasks  
   - Guarantees at-least-once delivery  

5. Retry Processor  
   - Tracks retry count  
   - Applies backoff strategy  
   - Emits failure metrics  
   - Dead-letter extension ready  

---

## 4. Tech Stack

- .NET 9
- StackExchange.Redis
- Serilog
- Prometheus
- NBomber
- Docker
- Kubernetes

---

## 5. Folder Structure (copyable tree format)

DistributedTaskQueue  
├── src  
│   ├── DistributedTaskQueue.Core  
│   ├── DistributedTaskQueue.Infrastructure  
│   ├── DistributedTaskQueue.Producer  
│   ├── DistributedTaskQueue.Worker  
│   └── DistributedTaskQueue.LoadTest  
├── k8s  
├── monitoring  
└── DistributedTaskQueue.sln  

---

## 6. Setup Instructions

1. Clone the repository

   git clone <repository-url>  
   cd DistributedTaskQueue  

2. Restore dependencies

   dotnet restore  

3. Run Redis locally (Docker)

   docker run -d -p 6379:6379 redis  

4. Run Producer

   dotnet run --project src/DistributedTaskQueue.Producer  

5. Run Worker

   dotnet run --project src/DistributedTaskQueue.Worker  

6. Run Load Test

   dotnet run --project src/DistributedTaskQueue.LoadTest -c Release  

---

## 7. API Endpoints

Worker exposes:

GET /health  
Returns application health status.

GET /metrics  
Exposes Prometheus metrics endpoint.

---

## 8. Key Features

- Visibility timeout with automatic recovery
- Retry scheduling using ZSET-based backoff
- Idempotency guard to prevent duplicate execution
- Priority queue support
- Circuit breaker protection
- Distributed rate limiting
- Distributed leadership lock
- Concurrency control per worker
- Atomic expired-task recovery
- Structured logging with Serilog
- Prometheus metrics integration
- Health endpoint for Kubernetes readiness and liveness

Delivery Semantics:

- At-least-once delivery
- Exactly-once execution via idempotent safeguards

---

## 9. Production Considerations

Scalability:

- Increase worker replicas in Kubernetes
- Increase per-worker concurrency
- Scale Redis vertically or horizontally
- Use Kubernetes Horizontal Pod Autoscaler

Backpressure Handling:

- Rate limiting
- Circuit breaker
- Controlled dequeue loop

Observability:

- Prometheus metrics:
  - dtq_tasks_processed_total
  - dtq_tasks_failed_total
  - dtq_tasks_retried_total
  - dtq_active_processing
  - Execution latency histogram

Grafana dashboard JSON available in:

monitoring/grafana-dashboard.json

Containerization:

Each service contains a multi-stage Dockerfile.

Example builds:

docker build -t dtq-worker ./src/DistributedTaskQueue.Worker  
docker build -t dtq-producer ./src/DistributedTaskQueue.Producer  

Kubernetes:

Manifests located in:

k8s/

Deploy:

kubectl apply -f k8s/

Includes:

- Redis deployment
- Worker deployment
- Service
- Horizontal Pod Autoscaler

---

## 10. Future Improvements

- Redis Streams backend implementation
- Dedicated dead-letter queue dashboard
- OpenTelemetry distributed tracing
- Multi-tenant isolation
- Persistent job history storage
- Web-based administration panel
- Advanced retry policies with dynamic backoff

---

## Summary

DistributedTaskQueue demonstrates:

- Distributed systems fundamentals
- Failure recovery strategies
- Concurrency control
- Horizontal scalability
- Production observability
- Infrastructure automation
- Clean architectural separation

It models a real-world background processing system designed for reliability, fault tolerance, and scale.
