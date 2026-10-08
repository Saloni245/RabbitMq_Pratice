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
4. **Resiliência Avançada (Retry, DLQ & Circuit Breaker)**:
   * Micro-recuperação com `UseMessageRetry`.
   * Isolamento de poison messages na Dead Letter Queue (`*_error`).
   * Proteção sistêmica com `UseCircuitBreaker` para evitar sobrecarga de downstream.
5. **Request / Response Assíncrono (`IRequestClient`)**:
   * Comunicação bidirecional assíncrona desacoplada sobre AMQP.
   * **Fila de Solicitação (Permanente)**: Ex: `VerificarCredito`. Escutada continuamente pelos consumidores.
   * **Fila de Retorno (Temporária / Auto-Delete)**: Criada dinamicamente pelo MassTransit com flags `exclusive=true` e `auto-delete=true`.
   * **Cabeçalhos AMQP (`ReplyTo` & `CorrelationId`)**: A resposta é endereçada diretamente de volta ao chamador correto, garantindo isolamento total de dados mesmo em ambientes multi-instância (scale-out horizontal).
   * 📝 **Nota Arquitetural (Revisitar / Deep Dive)**:
     - Gestão de concorrência e destruição de filas efêmeras em cenários de crash do cliente.
     - Ajuste fino de timeouts com `RequestTimeout.After(TimeSpan)`.
     - Tratamento polimórfico de respostas (`Response<T1, T2>`) e fallback em caso de `RequestTimeoutException`.

6. **Observabilidade com OpenTelemetry (Distributed Tracing & Métricas)**:
   * Coleta unificada de Spans com a fonte nativa do MassTransit (`AddSource("MassTransit")`) e métricas (`AddMeter("MassTransit")`).
   * Exportação padronizada via protocolo aberto **OTLP** (OpenTelemetry Protocol) para o **Jaeger** (`localhost:4317`).
   * Rastreabilidade fim a fim conectando Produtores, RabbitMQ Exchanges, Filas e Consumidores em um único `TraceId`.
   * 🐶 **Nota de Evolução Futura (Migração para DataDog / APM Corporativo)**:
     - Como a aplicação utiliza o padrão neutro **OpenTelemetry**, ela é 100% **Vendor-Agnostic** (independente de fornecedor).
     - **Zero Refatoração de Código**: Nenhuma linha de lógica de negócio ou consumer precisa ser alterada para migrar do Jaeger para o DataDog.
     - **Passo a Passo para Ativação do DataDog no Futuro**:
       1. **Subir o Datadog Agent** no Docker/Kubernetes com o receptor OTLP habilitado:
          - Variável: `DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT=0.0.0.0:4317`
          - Variável: `DD_API_KEY=<sua_api_key>` e `DD_SITE=datadoghq.com`
       2. **Redirecionar o Endpoint .NET**:
          - Alterar o endpoint no `AddOtlpExporter` (ou via variável de ambiente `OTEL_EXPORTER_OTLP_ENDPOINT`) apontando para o container do agente: `http://datadog-agent:4317`.
       3. **Aplicar Unified Service Tagging (Boas Práticas DataDog)**:
          - No `ConfigureResource`, incluir as tags padrão do DataDog: `env` (ex: `staging`, `production`) e `version` (ex: `1.0.0`).
       4. O DataDog gerará automaticamente o **Service Map**, gráficos de latência de fila, taxas de erro e correlação direta de Traces com Logs.

---

## 🐳 Infraestrutura Docker

O projeto disponibiliza compose files modulares para cada componente:

| Serviço | Arquivo Compose | Portas Expostas | Interface de Gestão |
| :--- | :--- | :--- | :--- |
| **RabbitMQ** | `RabbitMq_Pratice/Rabbit.yaml` | `5672` (AMQP), `15672` (HTTP) | `http://localhost:15672` |
| **Kafka + UI** | `RabbitMq_Pratice/kafka-compose.yml` | `9092` (Kafka), `8085` (UI) | `http://localhost:8085` |
| **PostgreSQL** | `RabbitMq_Pratice/postgres-compose.yml` | `5432` (Postgres) | CLI `psql` |
| **Observabilidade (LGTM)**<br>*(Jaeger, Prometheus, Grafana)* | `RabbitMq_Pratice/observability-compose.yml` | `16686` (Jaeger), `9090` (Prometheus), `3000` (Grafana), `4317` (OTLP) | Grafana: `http://localhost:3000`<br>Jaeger: `http://localhost:16686`<br>Prometheus: `http://localhost:9090` |

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

# Subir a Stack Completa de Observabilidade (Jaeger + Prometheus + Grafana)
docker compose -f RabbitMq_Pratice/observability-compose.yml up -d
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
- [x] Resiliência Avançada: Retry Policies, Dead Letter Queue (DLQ) e Circuit Breaker
- [x] Padrão de Comunicação Assíncrona Request / Response (`IRequestClient`)
- [x] Observabilidade de Mensageria (OpenTelemetry, Métricas Prometheus, Jaeger Tracing & Grafana)
- [ ] Orquestração de Saga State Machine
- [ ] Simulação de mensageria Cloud na AWS (LocalStack com Amazon SQS e SNS)

