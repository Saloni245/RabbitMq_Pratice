using MassTransit;
using Amazon.SQS;
using Amazon.SimpleNotificationService;
using System;
using System.Threading.Tasks;

namespace RabbitMq_Pratice
{
    // Evento específico para nuvem AWS
    public record NotificacaoCloudEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public string Origem { get; init; } = "AWS-LocalStack-SNS-SQS";
        public string Mensagem { get; init; } = string.Empty;
        public DateTime DataEnvio { get; init; } = DateTime.UtcNow;
    }

    public class NotificacaoCloudConsumer : IConsumer<NotificacaoCloudEvent>
    {
        public Task Consume(ConsumeContext<NotificacaoCloudEvent> context)
        {
            var msg = context.Message;
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"[AWS LocalStack SQS Consumer] ☁️ Mensagem recebida via Amazon SQS / SNS!");
            Console.WriteLine($"  -> ID: {msg.Id}");
            Console.WriteLine($"  -> Conteúdo: {msg.Mensagem}");
            Console.WriteLine($"  -> Origem: {msg.Origem}");
            Console.ResetColor();
            return Task.CompletedTask;
        }
    }
}

