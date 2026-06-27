/*
 * operacao.c — Orquestrador Saga do Sistema de Trading
 *
 * [Saga — Orquestração]
 * Este processo é o ORQUESTRADOR CENTRAL da Saga distribuída. Ele mantém
 * explicitamente o estado (EstadoSaga) e executa as etapas em sequência.
 * Diante de falha, dispara transações compensatórias para reverter o estado
 * do sistema (ex: venda do ETH/USDT para desfazer uma compra anterior).
 *
 * [Request-Reply]
 * Cada integração com um serviço usa uma conexão TCP dedicada:
 *   connect → send(requisição) → recv(resposta) → close
 * O orquestrador bloqueia em recv() aguardando a resposta.
 *
 * [Message Expiration]
 * A cotação recebida carrega timestamp_emissao_ms e ttl_ms. O orquestrador
 * valida a expiração em 3 pontos: antes do risco, antes da compra 1 e antes
 * da compra 2. Cotação expirada dispara a compensação quando necessário.
 *
 * Uso: ./operacao <N_ordens> [host_cotacao] [host_risco] [host_compra]
 * Ex:  ./operacao 5
 *      ./operacao 5 127.0.0.1 127.0.0.1 127.0.0.1
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocolo.h"

/* -----------------------------------------------------------------------
 * Contexto de configuração do orquestrador
 * ----------------------------------------------------------------------- */
typedef struct {
    int         n_ordens;
    const char *host_cotacao;
    const char *host_risco;
    const char *host_compra;
} ConfigOperacao;

/* -----------------------------------------------------------------------
 * [Request-Reply] Abre uma conexão TCP com o serviço alvo.
 * Retorna o fd do socket conectado, ou -1 em caso de erro.
 * ----------------------------------------------------------------------- */
static int conectar_servico(const char *host, int porta) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("[OPERACAO] Erro ao criar socket");
        return -1;
    }

    struct sockaddr_in destino;
    memset(&destino, 0, sizeof(destino));
    destino.sin_family = AF_INET;
    destino.sin_port   = htons((uint16_t)porta);

    if (inet_pton(AF_INET, host, &destino.sin_addr) <= 0) {
        fprintf(stderr, "[OPERACAO] Endereço inválido: %s\n", host);
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&destino, sizeof(destino)) < 0) {
        char msg[128];
        snprintf(msg, sizeof(msg), "Falha ao conectar em %s:%d", host, porta);
        perror(msg);
        close(fd);
        return -1;
    }

    return fd;
}

/* -----------------------------------------------------------------------
 * [Saga] Loga transição de estado com número de ordem para rastreabilidade
 * ----------------------------------------------------------------------- */
static void logar_transicao(int n_ordem, EstadoSaga estado, const char *detalhe) {
    char buffer[256];
    snprintf(buffer, sizeof(buffer),
             "[ORDEM-%d][%s] %s",
             n_ordem, nome_estado_saga(estado), detalhe);
    log_evento("OPERACAO", buffer);
}

/* -----------------------------------------------------------------------
 * [Saga + Request-Reply] Consulta o serviço de cotação.
 * Retorna 1 em sucesso (cotacao preenchida), 0 em falha.
 * ----------------------------------------------------------------------- */
static int consultar_cotacao(const char *host, int n_ordem, MsgCotacao *cotacao) {
    int fd = conectar_servico(host, PORTA_COTACAO);
    if (fd < 0) return 0;

    /* [Request-Reply] Aguarda a resposta bloqueado em recv() */
    ssize_t recebido = recv(fd, cotacao, sizeof(*cotacao), MSG_WAITALL);
    close(fd);

    if (recebido <= 0) {
        logar_transicao(n_ordem, SAGA_INICIADA,
                        "Erro de comunicação com serviço de cotação");
        return 0;
    }

    if (cotacao->status == STATUS_FALHA) {
        logar_transicao(n_ordem, SAGA_INICIADA,
                        "Serviço de cotação retornou falha");
        return 0;
    }

    return 1;
}

/* -----------------------------------------------------------------------
 * [Saga + Request-Reply] Consulta o serviço de risco.
 * Retorna 1 se aprovado, 0 se reprovado ou erro.
 * ----------------------------------------------------------------------- */
static int consultar_risco(const char *host, int n_ordem,
                           const MsgCotacao *cotacao) {
    int fd = conectar_servico(host, PORTA_RISCO);
    if (fd < 0) return 0;

    MsgRequisicaoRisco requisicao;
    memset(&requisicao, 0, sizeof(requisicao));
    requisicao.cotacao = *cotacao;

    /* [Request-Reply] Envia requisição e bloqueia aguardando resposta */
    ssize_t enviado = send(fd, &requisicao, sizeof(requisicao), 0);
    if (enviado < 0) {
        perror("[OPERACAO] Erro ao enviar requisição de risco");
        close(fd);
        return 0;
    }

    MsgRespostaRisco resposta;
    ssize_t recebido = recv(fd, &resposta, sizeof(resposta), MSG_WAITALL);
    close(fd);

    if (recebido <= 0) {
        logar_transicao(n_ordem, SAGA_RISCO_APROVADO,
                        "Erro de comunicação com serviço de risco");
        return 0;
    }

    return (resposta.status == STATUS_SUCESSO) ? 1 : 0;
}

/* -----------------------------------------------------------------------
 * [Saga + Request-Reply] Envia ordem ao serviço de compra.
 * Retorna 1 em sucesso, 0 em falha.
 * ----------------------------------------------------------------------- */
static int executar_ordem(const char *host, int n_ordem,
                          TipoOperacao tipo, const char *par, double preco) {
    int fd = conectar_servico(host, PORTA_COMPRA);
    if (fd < 0) return 0;

    MsgOrdemCompra ordem;
    memset(&ordem, 0, sizeof(ordem));
    ordem.tipo  = tipo;
    strncpy(ordem.par, par, sizeof(ordem.par) - 1);
    ordem.preco = preco;

    /* [Request-Reply] Envia ordem e bloqueia aguardando confirmação */
    ssize_t enviado = send(fd, &ordem, sizeof(ordem), 0);
    if (enviado < 0) {
        perror("[OPERACAO] Erro ao enviar ordem de compra");
        close(fd);
        return 0;
    }

    MsgRespostaCompra resposta;
    ssize_t recebido = recv(fd, &resposta, sizeof(resposta), MSG_WAITALL);
    close(fd);

    if (recebido <= 0) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "Erro de comunicação ao executar %s de %s",
                 (tipo == OP_COMPRA) ? "COMPRA" : "VENDA", par);
        logar_transicao(n_ordem, SAGA_FALHA_COMPRA1, msg);
        return 0;
    }

    return (resposta.status == STATUS_SUCESSO) ? 1 : 0;
}

/* -----------------------------------------------------------------------
 * [Saga] Executa transação compensatória: vende ETH/USDT para desfazer
 * a compra do ativo 1. Atualiza o estado da Saga para SAGA_COMPENSADA.
 * ----------------------------------------------------------------------- */
static void compensar_compra_eth(const char *host_compra, int n_ordem,
                                 double preco_eth, EstadoSaga *estado) {
    /*
     * [Saga] Transação compensatória: desfaz compra do ETH/USDT.
     * É disparada quando a cotação expira após a compra 1 ou quando
     * a compra 2 (USD/BRL) falha — ponto de não-retorno já ultrapassado.
     */
    int ok = executar_ordem(host_compra, n_ordem, OP_VENDA, "ETH/USDT", preco_eth);
    if (ok) {
        *estado = SAGA_COMPENSADA;
        logar_transicao(n_ordem, SAGA_COMPENSADA,
                        "Transação compensatória concluída: ETH/USDT vendido com sucesso");
    } else {
        /* Falha na compensação é crítica — loga como alerta grave */
        *estado = SAGA_COMPENSADA;
        logar_transicao(n_ordem, SAGA_COMPENSADA,
                        "ALERTA: falha na transação compensatória — intervenção manual necessária");
    }
}

/* -----------------------------------------------------------------------
 * [Saga] Executa uma ordem completa: cotação → risco → compra1 → compra2
 * Implementa o fluxo da Saga com todas as verificações de TTL e compensações.
 * ----------------------------------------------------------------------- */
static void executar_saga(const ConfigOperacao *cfg, int n_ordem) {
    EstadoSaga estado = SAGA_INICIADA;
    char buffer_log[256];

    printf("\n========== ORDEM %d — INÍCIO ==========\n", n_ordem);
    fflush(stdout);

    logar_transicao(n_ordem, SAGA_INICIADA, "Iniciando nova ordem de trading");

    /* -----------------------------------------------------------------------
     * Etapa 1: [Request-Reply] Obter cotação do mercado
     * ----------------------------------------------------------------------- */
    MsgCotacao cotacao;
    memset(&cotacao, 0, sizeof(cotacao));

    int ok = consultar_cotacao(cfg->host_cotacao, n_ordem, &cotacao);
    if (!ok) {
        estado = SAGA_FALHA_TTL; /* reutiliza estado de falha para erro de serviço */
        logar_transicao(n_ordem, estado, "Cotação não obtida — ordem abortada");
        goto fim_ordem;
    }

    estado = SAGA_COTACAO_RECEBIDA;
    snprintf(buffer_log, sizeof(buffer_log),
             "Cotação recebida: ETH/USDT=%.2f, USD/BRL=%.4f, TTL=%dms",
             cotacao.preco_eth_usdt, cotacao.preco_usd_brl, cotacao.ttl_ms);
    logar_transicao(n_ordem, estado, buffer_log);

    /* -----------------------------------------------------------------------
     * [Message Expiration] Verificação 1: antes de consultar o risco.
     * Se a cotação já expirou, a ordem é descartada (Dead Letter Channel).
     * ----------------------------------------------------------------------- */
    if (cotacao_expirada(&cotacao)) {
        estado = SAGA_FALHA_TTL;
        logar_transicao(n_ordem, estado,
                        "[TTL] Cotação expirada antes da análise de risco — ordem descartada");
        goto fim_ordem;
    }

    /* -----------------------------------------------------------------------
     * Etapa 2: [Request-Reply] Análise de risco (ponto pivô da Saga)
     * Após aprovação do risco, o sistema assume compromisso de completar ou
     * compensar — não é mais permitido simplesmente abandonar a operação.
     * ----------------------------------------------------------------------- */
    ok = consultar_risco(cfg->host_risco, n_ordem, &cotacao);
    if (!ok) {
        estado = SAGA_FALHA_RISCO;
        logar_transicao(n_ordem, estado,
                        "Risco reprovado — ordem abortada (sem compensação necessária)");
        goto fim_ordem;
    }

    estado = SAGA_RISCO_APROVADO;
    logar_transicao(n_ordem, estado,
                    "Risco aprovado — ponto pivô ultrapassado, operação comprometida");

    /* -----------------------------------------------------------------------
     * [Message Expiration] Verificação 2: antes da compra do ativo 1.
     * Nenhuma compra foi realizada ainda, então não há compensação necessária.
     * ----------------------------------------------------------------------- */
    if (cotacao_expirada(&cotacao)) {
        estado = SAGA_FALHA_TTL;
        logar_transicao(n_ordem, estado,
                        "[TTL] Cotação expirada antes da compra do ETH/USDT — ordem descartada");
        goto fim_ordem;
    }

    /* -----------------------------------------------------------------------
     * Etapa 3: [Request-Reply + Saga transação compensável]
     * Compra do ativo 1 (ETH/USDT) — transação compensável via OP_VENDA.
     * ----------------------------------------------------------------------- */
    ok = executar_ordem(cfg->host_compra, n_ordem,
                        OP_COMPRA, "ETH/USDT", cotacao.preco_eth_usdt);
    if (!ok) {
        estado = SAGA_FALHA_COMPRA1;
        logar_transicao(n_ordem, estado,
                        "Falha na compra do ETH/USDT — nada a compensar, ordem abortada");
        goto fim_ordem;
    }

    estado = SAGA_COMPRA1_REALIZADA;
    snprintf(buffer_log, sizeof(buffer_log),
             "ETH/USDT comprado @ %.2f — compra 1 realizada",
             cotacao.preco_eth_usdt);
    logar_transicao(n_ordem, estado, buffer_log);

    /* -----------------------------------------------------------------------
     * [Message Expiration] Verificação 3: antes da compra do ativo 2.
     * A compra 1 JÁ FOI REALIZADA — se o TTL expirou, é preciso compensar.
     * ----------------------------------------------------------------------- */
    if (cotacao_expirada(&cotacao)) {
        estado = SAGA_FALHA_TTL;
        logar_transicao(n_ordem, estado,
                        "[TTL] Cotação expirada após compra 1 — iniciando compensação");

        /* [Saga] Transação compensatória: desfaz compra do ETH/USDT */
        compensar_compra_eth(cfg->host_compra, n_ordem,
                             cotacao.preco_eth_usdt, &estado);
        goto fim_ordem;
    }

    /* -----------------------------------------------------------------------
     * Etapa 4: [Request-Reply + Saga transação compensatória]
     * Compra do ativo 2 (USD/BRL). Falha aqui exige compensação da compra 1.
     * ----------------------------------------------------------------------- */
    ok = executar_ordem(cfg->host_compra, n_ordem,
                        OP_COMPRA, "USD/BRL", cotacao.preco_usd_brl);
    if (!ok) {
        estado = SAGA_FALHA_COMPRA2;
        logar_transicao(n_ordem, estado,
                        "Falha na compra do USD/BRL — iniciando compensação da compra 1");

        /* [Saga] Transação compensatória: desfaz compra do ETH/USDT */
        compensar_compra_eth(cfg->host_compra, n_ordem,
                             cotacao.preco_eth_usdt, &estado);
        goto fim_ordem;
    }

    /* -----------------------------------------------------------------------
     * [Saga] Conclusão: todas as etapas executadas com sucesso.
     * ----------------------------------------------------------------------- */
    estado = SAGA_CONCLUIDA;
    snprintf(buffer_log, sizeof(buffer_log),
             "Ordem concluída: ETH/USDT @ %.2f + USD/BRL @ %.4f",
             cotacao.preco_eth_usdt, cotacao.preco_usd_brl);
    logar_transicao(n_ordem, estado, buffer_log);

fim_ordem:
    printf("========== ORDEM %d — ESTADO FINAL: %s ==========\n\n",
           n_ordem, nome_estado_saga(estado));
    fflush(stdout);
}

/* -----------------------------------------------------------------------
 * Ponto de entrada do orquestrador
 * ----------------------------------------------------------------------- */
int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Uso: %s <N_ordens> [host_cotacao] [host_risco] [host_compra]\n",
                argv[0]);
        fprintf(stderr, "Ex:  %s 5\n", argv[0]);
        fprintf(stderr, "Ex:  %s 5 127.0.0.1 127.0.0.1 127.0.0.1\n", argv[0]);
        return 1;
    }

    ConfigOperacao cfg;
    cfg.n_ordens     = atoi(argv[1]);
    cfg.host_cotacao = (argc >= 3) ? argv[2] : HOST_PADRAO;
    cfg.host_risco   = (argc >= 4) ? argv[3] : HOST_PADRAO;
    cfg.host_compra  = (argc >= 5) ? argv[4] : HOST_PADRAO;

    if (cfg.n_ordens <= 0) {
        fprintf(stderr, "[OPERACAO] Número de ordens deve ser positivo\n");
        return 1;
    }

    srand((unsigned int)(time(NULL) ^ getpid()));

    printf("[OPERACAO] Sistema de Trading iniciado\n");
    printf("[OPERACAO] Executando %d ordens\n", cfg.n_ordens);
    printf("[OPERACAO] Serviços: cotacao=%s:%d | risco=%s:%d | compra=%s:%d\n",
           cfg.host_cotacao, PORTA_COTACAO,
           cfg.host_risco,   PORTA_RISCO,
           cfg.host_compra,  PORTA_COMPRA);
    fflush(stdout);

    /* Loop principal: executa N ordens via Saga */
    for (int i = 1; i <= cfg.n_ordens; i++) {
        executar_saga(&cfg, i);
    }

    printf("[OPERACAO] Todas as ordens processadas. Encerrando.\n");
    return 0;
}
