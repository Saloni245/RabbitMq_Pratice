using MassTransit;

namespace RabbitMq_Pratice
{
    public class PedidoCriadoConsumer : IConsumer<PedidoCriadoEvent>
    {
        private readonly AppDbContext _dbContext;

        // Injeção de Dependência do EF Core
        public PedidoCriadoConsumer(AppDbContext dbContext)
        {
            _dbContext = dbContext;
        }

        public async Task Consume(ConsumeContext<PedidoCriadoEvent> context)
        {
            var msg = context.Message;
            Console.WriteLine($"[RabbitMQ Consumer] Mensagem recebida! Pedido: {msg.PedidoId}");

            // Grava o pedido no PostgreSQL
            var pedido = new Pedido
            {
                Id = msg.PedidoId,
                Valor = msg.Valor,
                Status = "Faturado",
                DataCriacao = msg.DataCriacao
            };

            _dbContext.Pedidos.Add(pedido);
            await _dbContext.SaveChangesAsync();

            Console.WriteLine($"[PostgreSQL] Pedido {msg.PedidoId} persistido com sucesso no banco!");
        }
    }
}