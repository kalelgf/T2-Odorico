#ifndef PROTOCOLO_H
#define PROTOCOLO_H

#include <stdio.h>
#include <time.h>

/* Portas e configurações de rede */
#define PORTA_COTACAO  8001
#define PORTA_RISCO    8002
#define PORTA_COMPRA   8003
#define HOST_PADRAO    "127.0.0.1"
#define BACKLOG        10

/* -----------------------------------------------------------------------
 * [Saga] Estados da transação distribuída gerenciada pelo orquestrador.
 * Cada transição é logada para rastreabilidade completa da Saga.
 * ----------------------------------------------------------------------- */
typedef enum {
    SAGA_INICIADA,
    SAGA_COTACAO_RECEBIDA,
    SAGA_RISCO_APROVADO,
    SAGA_COMPRA1_REALIZADA,
    SAGA_CONCLUIDA,
    SAGA_FALHA_TTL,
    SAGA_FALHA_RISCO,
    SAGA_FALHA_COMPRA1,
    SAGA_FALHA_COMPRA2,
    SAGA_COMPENSADA
} EstadoSaga;

/* Status genérico de resposta dos serviços */
typedef enum {
    STATUS_SUCESSO = 0,
    STATUS_FALHA   = 1
} StatusResposta;

/* Tipo de operação enviada ao serviço de compra */
typedef enum {
    OP_COMPRA = 0,
    OP_VENDA  = 1   /* [Saga] Usado na transação compensatória */
} TipoOperacao;

/* -----------------------------------------------------------------------
 * [Message Expiration] Mensagem de cotação com TTL embutido.
 * O campo timestamp_emissao_ms é preenchido pelo cotacao.c no momento
 * exato da geração da cotação (papel de Sender no padrão EIP).
 * O receptor (operacao.c) valida a expiração antes de cada etapa.
 * ----------------------------------------------------------------------- */
typedef struct {
    StatusResposta status;           /* STATUS_FALHA indica erro no serviço */
    double         preco_eth_usdt;
    double         preco_usd_brl;
    long           timestamp_emissao_ms; /* momento da geração (CLOCK_MONOTONIC) */
    int            ttl_ms;               /* tempo de vida configurável em ms */
} MsgCotacao;

/* Requisição ao serviço de risco */
typedef struct {
    MsgCotacao cotacao; /* cotação completa com TTL para revalidação */
} MsgRequisicaoRisco;

/* Resposta do serviço de risco */
typedef struct {
    StatusResposta status; /* STATUS_SUCESSO = aprovado, STATUS_FALHA = reprovado */
} MsgRespostaRisco;

/* -----------------------------------------------------------------------
 * [Request-Reply] Ordem enviada ao serviço de compra.
 * Cada envio abre uma conexão TCP dedicada: connect→send→recv→close.
 * ----------------------------------------------------------------------- */
typedef struct {
    TipoOperacao tipo;
    char         par[16]; /* ex: "ETH/USDT" ou "USD/BRL" */
    double       preco;
} MsgOrdemCompra;

/* Resposta do serviço de compra */
typedef struct {
    StatusResposta status;
} MsgRespostaCompra;

/* -----------------------------------------------------------------------
 * Funções utilitárias compartilhadas
 * ----------------------------------------------------------------------- */

/* Retorna o tempo atual em milissegundos usando CLOCK_MONOTONIC */
static inline long tempo_atual_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long)(ts.tv_sec * 1000L + ts.tv_nsec / 1000000L);
}

/* [Message Expiration] Retorna 1 se a cotação expirou, 0 se ainda válida */
static inline int cotacao_expirada(const MsgCotacao *cotacao) {
    long agora = tempo_atual_ms();
    return (agora - cotacao->timestamp_emissao_ms) > cotacao->ttl_ms;
}

/* Converte EstadoSaga para string legível */
static inline const char *nome_estado_saga(EstadoSaga estado) {
    switch (estado) {
        case SAGA_INICIADA:          return "INICIADA";
        case SAGA_COTACAO_RECEBIDA:  return "COTACAO_RECEBIDA";
        case SAGA_RISCO_APROVADO:    return "RISCO_APROVADO";
        case SAGA_COMPRA1_REALIZADA: return "COMPRA1_REALIZADA";
        case SAGA_CONCLUIDA:         return "CONCLUIDA";
        case SAGA_FALHA_TTL:         return "FALHA_TTL";
        case SAGA_FALHA_RISCO:       return "FALHA_RISCO";
        case SAGA_FALHA_COMPRA1:     return "FALHA_COMPRA1";
        case SAGA_FALHA_COMPRA2:     return "FALHA_COMPRA2";
        case SAGA_COMPENSADA:        return "COMPENSADA";
        default:                     return "DESCONHECIDO";
    }
}

/* Loga uma mensagem com timestamp e prefixo do processo */
static inline void log_evento(const char *processo, const char *mensagem) {
    long ms = tempo_atual_ms();
    printf("[%s][t=%ldms] %s\n", processo, ms, mensagem);
    fflush(stdout);
}

#endif /* PROTOCOLO_H */
