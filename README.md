# 🚀 Event-Driven Architecture (EDA) Playground (.NET 8)

> Laboratório prático de alta performance focado em **Arquitetura Orientada a Eventos (EDA)**, mensageria transacional, streaming de dados e consistência eventual com **.NET 8**, **RabbitMQ**, **Apache Kafka**, **MassTransit** e **PostgreSQL**.

---

## 📌 Visão Geral do Projeto

Este repositório foi construído com foco 80% prático e 20% teórico para demonstrar a aplicação de padrões corporativos do mundo real. O projeto contrasta as duas principais tecnologias de mensageria da indústria e implementa garantias de resiliência e integridade de dados.

```
                            ┌──────────────────────────────────────────────┐
                            │               Aplicação .NET 8               │
                            └───────┬──────────────────────────────┬───────┘
                                    │                              │
                    (AMQP / MassTransit)                  (Streaming / Log)
                                    │                              │
                                    ▼                              ▼
                         ┌────────────────────┐         ┌────────────────────┐
                         │      RabbitMQ      │         │    Apache Kafka    │
                         │   (Smart Broker)   │         │   (KRaft Cluster)  │
                         └──────────┬─────────┘         └──────────┬─────────┘
                                    │                              │
                                    ▼                              ▼
                          Consumidor com Inbox              Consumidor com
                        e Persistência Postgres          Offsets & Partições
                                    │
                                    ▼
                         ┌────────────────────┐
                         │     PostgreSQL     │
                         │   (Inbox / Outbox) │
                         └────────────────────┘
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
4. **MassTransit**:
   * Criação automática de topologia no RabbitMQ (Exchanges, Queues, Bindings).
   * Abstração de transporte corporativa.

---

## 🐳 Infraestrutura Docker

O projeto disponibiliza compose files modulares para cada componente:

| Serviço | Arquivo Compose | Portas Expostas | Interface de Gestão |
| :--- | :--- | :--- | :--- |
| **RabbitMQ** | `RabbitMq_Pratice/Rabbit.yaml` | `5672` (AMQP), `15672` (HTTP) | `http://localhost:15672` |
| **Kafka + UI** | `RabbitMq_Pratice/kafka-compose.yml` | `9092` (Kafka), `8085` (UI) | `http://localhost:8085` |
| **PostgreSQL** | `RabbitMq_Pratice/postgres-compose.yml` | `5432` (Postgres) | CLI `psql` |

---

## 🚀 Como Executar

### 1. Pré-requisitos
* [.NET 8 SDK ou superior](https://dotnet.microsoft.com/)
* [Docker Desktop](https://www.docker.com/)

### 2. Inicializar a Infraestrutura
Na raiz do projeto, suba os containers necessários:

```powershell
# Subir RabbitMQ
docker compose -f RabbitMq_Pratice/Rabbit.yaml up -d

# Subir Apache Kafka e Kafka UI
docker compose -f RabbitMq_Pratice/kafka-compose.yml up -d

# Subir PostgreSQL
docker compose -f RabbitMq_Pratice/postgres-compose.yml up -d
```

### 3. Rodar a Aplicação .NET
```powershell
dotnet run --project RabbitMq_Pratice/RabbitMq_Pratice.csproj
```

Pressione qualquer tecla no terminal para disparar o ciclo completo de mensageria e persistência.

---

## 🗺️ Roadmap de Evolução

- [x] Setup de infraestrutura em containers (RabbitMQ, Kafka KRaft, PostgreSQL)
- [x] Publicação e consumo com RabbitMQ + MassTransit
- [x] Produção e consumo com Apache Kafka (Particionamento e Offsets)
- [x] Persistência relacional com Entity Framework Core + Npgsql
- [x] Implementação de Transactional Outbox / Inbox Pattern
- [ ] Políticas de Retry com Exponential Backoff e Dead Letter Queue (DLQ)
- [ ] Orquestração de Saga State Machine
- [ ] Simulação de mensageria Cloud na AWS (LocalStack com Amazon SQS e SNS)

