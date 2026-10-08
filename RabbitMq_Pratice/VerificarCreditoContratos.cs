namespace RabbitMq_Pratice
{
    // A Pergunta (Request)
    public record VerificarCreditoRequest
    {
        public Guid ClienteId { get; init; }
        public decimal Valor { get; init; }
    }

    // Resposta de Sucesso
    public record CreditoAprovadoResponse
    {
        public Guid ClienteId { get; init; }
        public string Protocolo { get; init; } = string.Empty;
    }

    // Resposta de Recusa
    public record CreditoRecusadoResponse
    {
        public Guid ClienteId { get; init; }
        public string Motivo { get; init; } = string.Empty;
    }
}