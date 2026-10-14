using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMq_Pratice
{
    public class PedidoSagaMap : SagaClassMap<PedidoSagaState>
    {
        protected override void Configure(EntityTypeBuilder<PedidoSagaState> entity, ModelBuilder model)
        {
            entity.Property(x => x.CurrentState).HasMaxLength(64);
            entity.Property(x => x.Valor).HasPrecision(18, 2);
            entity.Property(x => x.MotivoCancelamento).HasMaxLength(256);
        }
    }

    public class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
        {
        }

        public DbSet<Pedido> Pedidos { get; set; }
        public DbSet<PedidoSagaState> PedidosSaga { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.AddInboxStateEntity();
            modelBuilder.AddOutboxMessageEntity();
            modelBuilder.AddOutboxStateEntity();

            // Configuração da persistência da Saga State Machine
            modelBuilder.Entity<PedidoSagaState>(entity =>
            {
                entity.HasKey(x => x.CorrelationId);
                entity.Property(x => x.CurrentState).HasMaxLength(64);
                entity.Property(x => x.Valor).HasPrecision(18, 2);
                entity.Property(x => x.MotivoCancelamento).HasMaxLength(256);
            });
        }
    }
}
