using Confluent.Kafka;
using System.Text.Json;

namespace RabbitMq_Pratice
{
    public static class KafkaProducerTest
    {
        public static async Task EnviarMensagemTesteAsync()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092" // 1. Onde está o Kafka?
            };

            using var producer = new ProducerBuilder<string, string>(config).Build();

            var evento = new PedidoCriadoEvent
            {
                PedidoId = Guid.NewGuid(),
                Valor = 250.00m,
                DataCriacao = DateTime.UtcNow
            };

            // 2. Chave do particionamento (ex: ID do cliente)
            string chave = "cliente-premium-101";

            // 3. Serializa o evento para JSON
            string json = JsonSerializer.Serialize(evento);

            Console.WriteLine("[Kafka] Enviando evento para o Kafka...");

            var entrega = await producer.ProduceAsync("pedidos-topico", new Message<string, string>
            {
                Key = chave,
                Value = json
            });

            Console.WriteLine($"[Kafka] Sucesso! Gravado no Tópico: {entrega.Topic} | Partição: {entrega.Partition.Value} | Offset: {entrega.Offset.Value}");
        }
    }
}