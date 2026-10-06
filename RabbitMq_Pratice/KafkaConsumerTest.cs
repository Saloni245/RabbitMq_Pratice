using Confluent.Kafka;
using System.Text.Json;

namespace RabbitMq_Pratice
{
    public static class KafkaConsumerTest
    {
        public static async Task IniciarConsumoAsync(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "servico-faturamento-v1",
                AutoOffsetReset = AutoOffsetReset.Earliest, // Lê desde o início do tópico!
                EnableAutoCommit = true // Confirma o offset automaticamente após ler
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            // 1. Assina o tópico
            consumer.Subscribe("pedidos-topico");

            Console.WriteLine("[Kafka Consumer] Conectado e aguardando eventos do tópico...");

            // 2. Loop contínuo de consumo (Polling)
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Faz polling no broker com timeout de 1 segundo
                    var resultado = consumer.Consume(TimeSpan.FromSeconds(1));

                    if (resultado != null)
                    {
                        var evento = JsonSerializer.Deserialize<PedidoCriadoEvent>(resultado.Message.Value);

                        Console.WriteLine($"[Kafka Consumer] Mensagem lida com sucesso!");
                        Console.WriteLine($" -> Chave: {resultado.Message.Key}");
                        Console.WriteLine($" -> Pedido ID: {evento?.PedidoId} | Valor: R$ {evento?.Valor:F2}");
                        Console.WriteLine($" -> Partição: {resultado.Partition.Value} | Offset: {resultado.Offset.Value}");
                        Console.WriteLine("--------------------------------------------------");
                    }
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"[Kafka Consumer Erro]: {ex.Error.Reason}");
                }
            }
            // Configura o cancelamento com Ctrl + C
            using var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
            // Inicia o consumidor do Kafka em loop contínuo
            await KafkaConsumerTest.IniciarConsumoAsync(cts.Token);
        }

    }
}