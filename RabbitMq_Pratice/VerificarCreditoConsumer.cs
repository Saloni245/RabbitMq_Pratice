using MassTransit;

namespace RabbitMq_Pratice
{
    public class VerificarCreditoConsumer : IConsumer<VerificarCreditoRequest>
    {
        public async Task Consume(ConsumeContext<VerificarCreditoRequest> context)
        {
            var request = context.Message;
            Console.WriteLine($"[Credito Service] Analisando crédito de R$ {request.Valor:F2} para o cliente {request.ClienteId}...");

            // Se houvesse I/O demorado ou chamada externa, passaríamos context.CancellationToken:
            // await _bancoService.ConsultarAsync(request.ClienteId, context.CancellationToken);

            if (request.Valor >= 1000m)
            {
                // Responde com Recusa
                await context.RespondAsync<CreditoRecusadoResponse>(new CreditoRecusadoResponse
                {
                    ClienteId = request.ClienteId,
                    Motivo = "Valor excede o limite pré-aprovado de R$ 1.000,00"
                });

                Console.WriteLine("[Credito Service] Crédito RECUSADO.");
            }
            else
            {
                // Responde com Aprovação
                await context.RespondAsync<CreditoAprovadoResponse>(new CreditoAprovadoResponse
                {
                    ClienteId = request.ClienteId,
                    Protocolo = $"APROV-{Guid.NewGuid().ToString()[..8].ToUpper()}"
                });

                Console.WriteLine("[Credito Service] Crédito APROVADO.");
            }
        }
    }
}