using MassTransit;
using System;

namespace RabbitMq_Pratice
{
    // Estado da Saga persistido
    public class PedidoSagaState : SagaStateMachineInstance
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public decimal Valor { get; set; }
        public DateTime DataCriacao { get; set; }
        public DateTime? DataConclusao { get; set; }
        public string? MotivoCancelamento { get; set; }
    }

    // Eventos e Comandos da Saga
    public record IniciarProcessamentoPedidoEvent
    {
        public Guid PedidoId { get; init; }
        public decimal Valor { get; init; }
    }

    public record EstoqueReservadoEvent
    {
        public Guid PedidoId { get; init; }
    }

    public record EstoqueEsgotadoEvent
    {
        public Guid PedidoId { get; init; }
        public string Motivo { get; init; } = string.Empty;
    }

    public record PagamentoAprovadoEvent
    {
        public Guid PedidoId { get; init; }
    }

    // State Machine
    public class PedidoStateMachine : MassTransitStateMachine<PedidoSagaState>
    {
        public State AguardandoEstoque { get; private set; } = null!;
        public State AguardandoPagamento { get; private set; } = null!;
        public State Concluido { get; private set; } = null!;
        public State Cancelado { get; private set; } = null!;

        public Event<IniciarProcessamentoPedidoEvent> PedidoIniciado { get; private set; } = null!;
        public Event<EstoqueReservadoEvent> EstoqueReservado { get; private set; } = null!;
        public Event<EstoqueEsgotadoEvent> EstoqueEsgotado { get; private set; } = null!;
        public Event<PagamentoAprovadoEvent> PagamentoAprovado { get; private set; } = null!;

        public PedidoStateMachine()
        {
            InstanceState(x => x.CurrentState);

            Event(() => PedidoIniciado, x => x.CorrelateById(m => m.Message.PedidoId));
            Event(() => EstoqueReservado, x => x.CorrelateById(m => m.Message.PedidoId));
            Event(() => EstoqueEsgotado, x => x.CorrelateById(m => m.Message.PedidoId));
            Event(() => PagamentoAprovado, x => x.CorrelateById(m => m.Message.PedidoId));

            // Fluxo Inicial
            Initially(
                When(PedidoIniciado)
                    .Then(context =>
                    {
                        context.Saga.Valor = context.Message.Valor;
                        context.Saga.DataCriacao = DateTime.UtcNow;
                        Console.WriteLine($"[Saga Orchestrator] 🚀 Iniciada Saga para o Pedido: {context.Saga.CorrelationId} (R$ {context.Saga.Valor})");
                    })
                    .TransitionTo(AguardandoEstoque)
            );

            // Durante AguardandoEstoque
            During(AguardandoEstoque,
                When(EstoqueReservado)
                    .Then(context => Console.WriteLine($"[Saga Orchestrator] 📦 Estoque Reservado para o Pedido: {context.Saga.CorrelationId}. Aguardando Pagamento..."))
                    .TransitionTo(AguardandoPagamento),
                When(EstoqueEsgotado)
                    .Then(context =>
                    {
                        context.Saga.MotivoCancelamento = context.Message.Motivo;
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"[Saga Orchestrator] ❌ Estoque Esgotado! Compensando e cancelando pedido: {context.Saga.CorrelationId}. Motivo: {context.Message.Motivo}");
                        Console.ResetColor();
                    })
                    .TransitionTo(Cancelado)
            );

            // Durante AguardandoPagamento
            During(AguardandoPagamento,
                When(PagamentoAprovado)
                    .Then(context =>
                    {
                        context.Saga.DataConclusao = DateTime.UtcNow;
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"[Saga Orchestrator] 💰 Pagamento Confirmado! Pedido: {context.Saga.CorrelationId} CONCLUÍDO com sucesso!");
                        Console.ResetColor();
                    })
                    .TransitionTo(Concluido)
            );
        }
    }
}

