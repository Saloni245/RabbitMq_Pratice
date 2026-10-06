using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMq_Pratice
{
    public record PedidoCriadoEvent
    {
        public Guid PedidoId { get; init; }
        public decimal Valor { get; init; }
        public DateTime DataCriacao { get; init; }
    }
}
