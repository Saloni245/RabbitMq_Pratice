using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMq_Pratice;

using Microsoft.Extensions.Logging;
var builder = Host.CreateApplicationBuilder(args);
// Silencia as queries SQL do Entity Framework no Console
builder.Logging.AddFilter("Microsoft.EntityFrameworkCore.Database.Command", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.EntityFrameworkCore", LogLevel.Warning);

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

        cfg.ConfigureEndpoints(context);
    });
});

var app = builder.Build();

// 3. Cria o banco e as tabelas (Pedidos, InboxState, OutboxState) se ainda não existirem
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    await db.Database.EnsureCreatedAsync();
    Console.WriteLine("[Database] Banco e tabelas do Inbox/Outbox sincronizados no Postgres!");
}

await app.StartAsync();

Console.WriteLine("MassTransit conectado ao RabbitMQ e PostgreSQL pronto!");
Console.WriteLine("Pressione qualquer tecla para publicar um pedido de teste...");
Console.ReadKey();

// 4. Publica o pedido
// 4. Publica o pedido DENTRO do escopo do banco (Transactional Outbox)
using (var scope = app.Services.CreateScope())
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
await app.WaitForShutdownAsync();