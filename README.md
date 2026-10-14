# 🚀 Event-Driven Architecture (EDA) Playground (.NET 8)

> Laboratório prático de alta performance focado em **Arquitetura Orientada a Eventos (EDA)**, mensageria transacional, streaming de dados e consistência eventual com **.NET 8**, **RabbitMQ**, **Apache Kafka**, **MassTransit**, **AWS (LocalStack)** e **PostgreSQL**.

---

## 📌 Visão Geral da Arquitetura

Este repositório foi construído com foco 80% prático e 20% teórico para demonstrar a aplicação de padrões corporativos do mundo real. O projeto contrasta as principais tecnologias de mensageria e streaming da indústria, com orquestração de Saga, nuvem AWS e observabilidade ponta a ponta.

```
                                    ┌────────────────────────────────────────────────────────┐
                                    │                    Aplicação .NET 8                    │
                                    │ (MassTransit + Confluent.Kafka + AWSSDK + OpenTelemetry│
                                    └───────┬───────────────────┬────────────────────┬───────┘
                                            │                   │                    │
                            (AMQP / MassTransit)        (Streaming / Log)      (AWS Cloud SQS/SNS)
                                            │                   │                    │
                                            ▼                   ▼                    ▼
                                 ┌────────────────────┐ ┌───────────────┐ ┌────────────────────┐
                                 │      RabbitMQ      │ │ Apache Kafka  │ │   AWS LocalStack   │
                                 │   (Smart Broker)   │ │(KRaft Cluster)│ │    (SQS & SNS)     │
                                 └──────────┬─────────┘ └───────┬───────┘ └──────────┬─────────┘
                                            │                   │                    │
                                            ▼                   ▼                    ▼
                                      Consumidores         Consumidores         Consumidores
                                      & Saga State      Offsets / Partições     Nuvem Híbrida
                                            │
                                            ▼
                                 ┌────────────────────────────────────────┐
                                 │               PostgreSQL               │
                                 │ (Transactional Outbox/Inbox + Saga DB) │
                                 └────────────────────────────────────────┘
                                                    ▲
                                                    │ (Tracing / Métricas OTLP)
                                 ┌──────────────────┴─────────────────────┐
                                 │       Stack Observabilidade LGTM       │
                                 │     (Jaeger + Prometheus + Grafana)    │
                                 └────────────────────────────────────────┘
```

---

## ⚖️ RabbitMQ vs. Apache Kafka (Trade-offs de Arquitetura)

| Característica | RabbitMQ (com MassTransit) | Apache Kafka (Confluent.Kafka) |
| :--- | :--- | :--- |
| **Paradigma** | **Message Broker** clássico (Smart Broker, Dumb Consumer) | **Distributed Event Streaming** (Dumb Broker, Smart Consumer) |
| **Retenção** | Transiente: mensagem é deletada após o `ACK` do consumidor | Persistente: log imutável append-only retido em disco |
| **Garantia de Ordem** | Difícil em consumidores concorrentes | Estrita por chave de partição (`Message Key`) |
| **Replay de Eventos** | Não suportado (a mensagem sumiu) | Suportado via manipulação de `Offset` (`AutoOffsetReset.Earliest`) |
| **Caso de Uso Ideal** | Transações, comandos de negócio, workflows, DLQs | Auditoria, telemetria, streaming contínuo, event sourcing |

---

## 🛠️ Padrões e Tecnologias Implementados

1. **Transactional Outbox Pattern**:
   * Resolve o problema de *Dual-Write*.
   * Eventos e entidades de negócio são salvos na mesma transação atômica do PostgreSQL via EF Core antes da postagem no broker.
2. **Inbox Pattern (Idempotência)**:
   * Evita processamento duplicado garantindo a premissa de *At-Least-Once Delivery*.
3. **Kafka KRaft Mode**:
   * Cluster Kafka moderno sem dependência do Apache ZooKeeper.
   * Multi-listener configurado para tráfego interno (Docker) e externo (Host Windows).
4. **Resiliência Avançada (Retry, DLQ & Circuit Breaker)**:
   * Micro-recuperação com `UseMessageRetry`.
   * Isolamento de poison messages na Dead Letter Queue (`*_error`).
   * Proteção sistêmica com `UseCircuitBreaker` para evitar sobrecarga de downstream.
5. **Request / Response Assíncrono (`IRequestClient`)**:
   * Comunicação bidirecional assíncrona desacoplada sobre AMQP.
   * **Fila de Solicitação (Permanente)**: Ex: `VerificarCredito`. Escutada continuamente pelos consumidores.
   * **Fila de Retorno (Temporária / Auto-Delete)**: Criada dinamicamente pelo MassTransit com flags `exclusive=true` e `auto-delete=true`.
   * **Cabeçalhos AMQP (`ReplyTo` & `CorrelationId`)**: A resposta é endereçada diretamente de volta ao chamador correto, garantindo isolamento total de dados mesmo em ambientes multi-instância (scale-out horizontal).
   * ✅ **Resiliência e Timeout Implementados**:
     - Destruição e isolamento automático de filas efêmeras (`exclusive=true`, `auto-delete=true`).
     - Controle rígido de timeout com `RequestTimeout.After(s: 5)`.
     - Resposta polimórfica (`Response<T1, T2>`) protegida com contingência/fallback gracioso via `RequestTimeoutException`.

6. **Orquestração de Processos com Saga State Machine**:
   * Gerenciamento de processos distribuídos de longa duração com **MassTransit State Machine**.
   * Ciclo de vida completo: `Iniciar Pedido` ➔ `Reservar Estoque` ➔ `Confirmar Pagamento` com compensação atômica em caso de falha.
   * Persistência de estado correlacionado (`CorrelationId`) diretamente no **PostgreSQL** via Entity Framework Core.

7. **Nuvem Híbrida AWS com LocalStack (SQS / SNS)**:
   * Simulação local de mensageria corporativa da AWS com **Amazon SQS** e **SNS**.
   * Criação programática de filas, despacho de eventos na nuvem e consumo desacoplado com o SDK oficial da AWS.

8. **Observabilidade com OpenTelemetry (Distributed Tracing & Métricas)**:
   * Coleta unificada de Spans com a fonte nativa do MassTransit (`AddSource("MassTransit")`) e métricas (`AddMeter("MassTransit")`).
   * Exportação padronizada via protocolo aberto **OTLP** (OpenTelemetry Protocol) para o **Jaeger** (`localhost:4317`).
   * Rastreabilidade fim a fim conectando Produtores, RabbitMQ Exchanges, Filas e Consumidores em um único `TraceId`.
   * 🐶 **Vendor-Agnostic (DataDog / APM)**: 100% compatível com APMs corporativos sem alteração de código.

---

## 🐳 Infraestrutura Docker

O projeto disponibiliza compose files modulares para cada componente:

| Serviço | Arquivo Compose | Portas Expostas | Interface de Gestão |
| :--- | :--- | :--- | :--- |
| **RabbitMQ** | `RabbitMq_Pratice/Rabbit.yaml` | `5672` (AMQP), `15672` (HTTP) | `http://localhost:15672` |
| **Kafka + UI** | `RabbitMq_Pratice/kafka-compose.yml` | `9092` (Kafka), `8085` (UI) | `http://localhost:8085` |
| **PostgreSQL** | `RabbitMq_Pratice/postgres-compose.yml` | `5432` (Postgres) | CLI `psql` |
| **LocalStack AWS (SQS/SNS)** | `RabbitMq_Pratice/localstack-compose.yml` | `4566` (AWS Gateway) | CLI / SDK `http://localhost:4566` |
| **Observabilidade (LGTM)**<br>*(Jaeger, Prometheus, Grafana)* | `RabbitMq_Pratice/observability-compose.yml` | `16686` (Jaeger), `9090` (Prometheus), `3000` (Grafana), `4317` (OTLP) | Grafana: `http://localhost:3000`<br>Jaeger: `http://localhost:16686`<br>Prometheus: `http://localhost:9090` |

---

## 🚀 Como Executar

### 1. Inicializar a Infraestrutura Completa
Na raiz do projeto, suba os containers necessários:

```powershell
# Subir RabbitMQ, Kafka, Postgres, LocalStack e Observabilidade
docker compose -f RabbitMq_Pratice/Rabbit.yaml up -d
docker compose -f RabbitMq_Pratice/kafka-compose.yml up -d
docker compose -f RabbitMq_Pratice/postgres-compose.yml up -d
docker compose -f RabbitMq_Pratice/localstack-compose.yml up -d
docker compose -f RabbitMq_Pratice/observability-compose.yml up -d
```

### 2. Rodar a Aplicação .NET
```powershell
dotnet run --project RabbitMq_Pratice/RabbitMq_Pratice.csproj
```

---

## 🗺️ Roadmap de Evolução

- [x] Setup de infraestrutura em containers (RabbitMQ, Kafka KRaft, PostgreSQL)
- [x] Publicação e consumo com RabbitMQ + MassTransit
- [x] Produção e consumo com Apache Kafka (Particionamento e Offsets)
- [x] Persistência relacional com Entity Framework Core + Npgsql
- [x] Implementação de Transactional Outbox / Inbox Pattern
- [x] Resiliência Avançada: Retry Policies, Dead Letter Queue (DLQ) e Circuit Breaker
- [x] Padrão de Comunicação Assíncrona Request / Response (`IRequestClient`)
- [x] Observabilidade de Mensageria (OpenTelemetry, Métricas Prometheus, Jaeger Tracing & Grafana)
- [x] Orquestração de Saga State Machine (MassTransit Automatonymous / State Machine)
- [x] Simulação de mensageria Cloud na AWS (LocalStack com Amazon SQS e SNS)
