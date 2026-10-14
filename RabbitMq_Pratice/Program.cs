using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMq_Pratice;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Amazon.SQS;
using Amazon.SimpleNotificationService;
using Amazon.Runtime;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.AddFilter("Microsoft.EntityFrameworkCore.Database.Command", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.EntityFrameworkCore", LogLevel.Warning);

// 0. Observabilidade com OpenTelemetry (Distributed Tracing e Métricas)
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource
        .AddService(serviceName: "RabbitMq_Pratice_EDA", serviceVersion: "1.0.0"))
    .WithTracing(tracing =>
    {
        tracing
            .AddSource("MassTransit")
            .AddOtlpExporter(options =>
            {
                options.Endpoint = new Uri("http://localhost:4317"); // Jaeger
            });
    })
    .WithMetrics(metrics =>
    {
        metrics
            .AddMeter("MassTransit")
            .AddOtlpExporter(options =>
            {
                options.Endpoint = new Uri("http://localhost:4317");
            });
    });

// 1. Registra a Conexão com o PostgreSQL
var connectionString = "Host=localhost;Port=5432;Database=app_db;Username=postgres;Password=password123";
builder.Services.AddDbContext<AppDbContext>(options =>
{
    options.UseNpgsql(connectionString);
});

// 2. Configura o MassTransit com RabbitMQ, Outbox e Saga State Machine
builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<PedidoCriadoConsumer>();
    x.AddConsumer<VerificarCreditoConsumer>();

    // SAGA STATE MACHINE
    x.AddSagaStateMachine<PedidoStateMachine, PedidoSagaState>()
        .EntityFrameworkRepository(r =>
        {
            r.ExistingDbContext<AppDbContext>();
            r.UsePostgres();
        });

    // ATIVAÇÃO DO OUTBOX / INBOX PATTERN
    x.AddEntityFrameworkOutbox<AppDbContext>(o =>
    {
        o.UsePostgres();
        o.UseBusOutbox();
    });

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.Host("localhost", "/", h =>
        {
            h.Username("nicholas");
            h.Password("0360");
        });
        cfg.UseMessageRetry(r => r.Interval(3, TimeSpan.FromSeconds(3)));
        cfg.UseCircuitBreaker(cb =>
        {
            cb.TrackingPeriod = TimeSpan.FromMinutes(1);
            cb.TripThreshold = 15;
            cb.ActiveThreshold = 5;
            cb.ResetInterval = TimeSpan.FromMinutes(1);
        });
        cfg.ConfigureEndpoints(context);
    });
});

var app = builder.Build();

// 3. Cria o banco e as tabelas (Pedidos, Saga, Inbox/Outbox)
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    await db.Database.EnsureCreatedAsync();
    Console.WriteLine("[Database] Banco, tabelas de Saga e Inbox/Outbox sincronizados no Postgres!");
}

await app.StartAsync();

Console.WriteLine("\n========================================================");
Console.WriteLine("🚀 EDA Playground .NET 8 Inicializado com Sucesso!");
Console.WriteLine("========================================================\n");

// 4. Teste Transactional Outbox com RabbitMQ
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    var publishEndpoint = scope.ServiceProvider.GetRequiredService<IPublishEndpoint>();

    var pedidoId = Guid.NewGuid();
    await publishEndpoint.Publish(new PedidoCriadoEvent
    {
        PedidoId = pedidoId,
        Valor = 350.00m,
        DataCriacao = DateTime.UtcNow
    });

    await db.SaveChangesAsync();
    Console.WriteLine($"[Outbox] Pedido {pedidoId} gravado atomicamente no Postgres e despachado para o RabbitMQ!");
}

// 5. Teste Request/Response com Timeout Seguro
var client = app.Services.GetRequiredService<IRequestClient<VerificarCreditoRequest>>();

Console.WriteLine("\n--- TESTE Request/Response: Solicitando crédito de R$ 500,00 ---");
try
{
    var resposta1 = await client.GetResponse<CreditoAprovadoResponse, CreditoRecusadoResponse>(
        new VerificarCreditoRequest
        {
            ClienteId = Guid.NewGuid(),
            Valor = 500.00m
        },
        timeout: RequestTimeout.After(s: 5));

    if (resposta1.Is(out Response<CreditoAprovadoResponse>? aprovado1))
    {
        Console.WriteLine($"[RequestClient] -> APROVADO! Protocolo: {aprovado1.Message.Protocolo}");
    }
}
catch (RequestTimeoutException ex)
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($"[Timeout/Fallback] Resposta demorou mais de 5s: {ex.Message}");
    Console.ResetColor();
}

// 6. Teste Saga State Machine
Console.WriteLine("\n--- TESTE Saga State Machine (Orquestração de Pedido) ---");
var sagaBus = app.Services.GetRequiredService<IPublishEndpoint>();
var sagaPedidoId = Guid.NewGuid();

// Dispara início da saga
await sagaBus.Publish(new IniciarProcessamentoPedidoEvent
{
    PedidoId = sagaPedidoId,
    Valor = 1200.00m
});

await Task.Delay(1000);

// Dispara reserva de estoque
await sagaBus.Publish(new EstoqueReservadoEvent
{
    PedidoId = sagaPedidoId
});

await Task.Delay(1000);

// Dispara confirmação de pagamento
await sagaBus.Publish(new PagamentoAprovadoEvent
{
    PedidoId = sagaPedidoId
});

await Task.Delay(1500);

// 7. Simulação AWS SQS e SNS com LocalStack
Console.WriteLine("\n--- TESTE Nuvem AWS: Publicação em SQS / SNS via LocalStack ---");
try
{
    var credentials = new BasicAWSCredentials("test", "test");
    var sqsConfig = new AmazonSQSConfig
    {
        ServiceURL = "http://localhost:4566",
        UseHttp = true,
        AuthenticationRegion = "us-east-1"
    };

    using var sqsClient = new AmazonSQSClient(credentials, sqsConfig);
    var queueUrlResponse = await sqsClient.CreateQueueAsync("pedidos-cloud-queue");
    var queueUrl = queueUrlResponse.QueueUrl;

    await sqsClient.SendMessageAsync(new Amazon.SQS.Model.SendMessageRequest
    {
        QueueUrl = queueUrl,
        MessageBody = $"{{\"evento\": \"PedidoProcessadoCloud\", \"pedidoId\": \"{sagaPedidoId}\", \"timestamp\": \"{DateTime.UtcNow:O}\"}}"
    });

    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine($"[AWS SQS / LocalStack] ☁️ Mensagem despachada com sucesso na fila SQS '{queueUrl}'!");
    
    // Leitura da mensagem na fila SQS
    var receiveResponse = await sqsClient.ReceiveMessageAsync(new Amazon.SQS.Model.ReceiveMessageRequest
    {
        QueueUrl = queueUrl,
        MaxNumberOfMessages = 1,
        WaitTimeSeconds = 2
    });

    foreach (var message in receiveResponse.Messages)
    {
        Console.WriteLine($"[AWS SQS / LocalStack] 📬 Mensagem consumida da fila SQS: {message.Body}");
        await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle);
    }
    Console.ResetColor();
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($"[AWS LocalStack] Aviso (Container ainda inicializando ou offline): {ex.Message}");
    Console.ResetColor();
}

Console.WriteLine("\n✅ Todas as etapas de arquitetura EDA executadas com maestria!");
Console.WriteLine("Pressione Ctrl+C para encerrar.");
await app.WaitForShutdownAsync();