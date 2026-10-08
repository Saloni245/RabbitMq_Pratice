using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMq_Pratice;

using Microsoft.Extensions.Logging;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = Host.CreateApplicationBuilder(args);
// Silencia as queries SQL do Entity Framework no Console
builder.Logging.AddFilter("Microsoft.EntityFrameworkCore.Database.Command", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.EntityFrameworkCore", LogLevel.Warning);

// 0. Observabilidade com OpenTelemetry (Distributed Tracing e Métricas)
// Dica de Arquiteto (Evolução DataDog/APM):
// O OpenTelemetry é neutro (vendor-agnostic). Hoje exportamos para o Jaeger local (http://localhost:4317).
// No futuro, para plugar no DataDog, basta apontar o Endpoint para o Datadog Agent OTLP (ex: http://datadog-agent:4317)
// sem alterar nenhuma linha de negócio ou nos consumidores!
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource
        .AddService(serviceName: "RabbitMq_Pratice_EDA", serviceVersion: "1.0.0"))
    .WithTracing(tracing =>
    {
        tracing
            .AddSource("MassTransit") // Instrumentação nativa do MassTransit
            .AddOtlpExporter(options =>
            {
                options.Endpoint = new Uri("http://localhost:4317"); // Jaeger (local) ou Datadog Agent
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

// 2. Configura o MassTransit com Inbox/Outbox no EF Core
builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<PedidoCriadoConsumer>();
    x.AddConsumer<VerificarCreditoConsumer>();

    // ATIVAÇÃO DO OUTBOX / INBOX PATTERN
    x.AddEntityFrameworkOutbox<AppDbContext>(o =>
    {
        o.UsePostgres();
        o.UseBusOutbox(); // Habilita o Transactional Outbox
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

// 3. Cria o banco e as tabelas (Pedidos, InboxState, OutboxState) se ainda não existirem
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    await db.Database.EnsureCreatedAsync();
    Console.WriteLine("[Database] Banco e tabelas do Inbox/Outbox sincronizados no Postgres!");
}

await app.StartAsync();

Console.WriteLine("MassTransit conectado ao RabbitMQ e PostgreSQL pronto!");
Console.WriteLine("Pressione qualquer tecla para publicar um pedido de teste...");
Console.ReadKey();

// 4. Publica o pedido DENTRO do escopo do banco (Transactional Outbox)
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

    // A MÁGICA ACONTECE AQUI:
    // O SaveChanges confirma a transação no Postgres. 
    // O MassTransit grava na tabela "OutboxMessage" e despacha imediatamente para o RabbitMQ!
    await db.SaveChangesAsync();

    Console.WriteLine($"--> Pedido {pedidoId} confirmado no banco e despachado pelo Outbox!");
}
// Mantém vivo escutando
// 5. Padrão Request/Response Assíncrono com IRequestClient
var client = app.Services.GetRequiredService<IRequestClient<VerificarCreditoRequest>>();
Console.WriteLine("\n--- TESTE 1: Solicitando crédito de R$ 500,00 ---");
var resposta1 = await client.GetResponse<CreditoAprovadoResponse, CreditoRecusadoResponse>(new VerificarCreditoRequest
{
    ClienteId = Guid.NewGuid(),
    Valor = 500.00m
});
if (resposta1.Is(out Response<CreditoAprovadoResponse>? aprovado1))
{
    Console.WriteLine($"[Cliente Recebeu] -> APROVADO! Protocolo: {aprovado1.Message.Protocolo}");
}
Console.WriteLine("\n--- TESTE 2: Solicitando crédito de R$ 1.500,00 ---");
var resposta2 = await client.GetResponse<CreditoAprovadoResponse, CreditoRecusadoResponse>(new VerificarCreditoRequest
{
    ClienteId = Guid.NewGuid(),
    Valor = 1500.00m
});
if (resposta2.Is(out Response<CreditoRecusadoResponse>? recusado2))
{
    Console.WriteLine($"[Cliente Recebeu] -> RECUSADO! Motivo: {recusado2.Message.Motivo}");
}
await app.WaitForShutdownAsync();