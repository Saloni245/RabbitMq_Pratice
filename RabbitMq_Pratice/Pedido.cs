using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMq_Pratice
{
    public class Pedido
    {
        public Guid Id { get; set; }
        public decimal Valor { get; set; }
        public string Status { get; set; } = string.Empty;
        public DateTime DataCriacao { get; set; }
    }
}
