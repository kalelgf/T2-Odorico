/*
 * compra.c — Servidor de Compras (porta 8003)
 *
 * [Request-Reply — papel de Replier]
 * Recebe MsgOrdemCompra (compra ou venda), processa e retorna MsgRespostaCompra.
 *
 * [Saga — transação compensatória]
 * Operações do tipo OP_VENDA representam transações compensatórias disparadas
 * pelo orquestrador (operacao.c) para desfazer uma compra anterior.
 * Por simplificação válida do protótipo, vendas (compensações) SEMPRE retornam
 * sucesso — na prática real, uma venda a mercado pode ter latência mas raramente
 * falha de forma definitiva, especialmente em janelas de compensação curtas.
 *
 * Uso: ./compra <taxa_sucesso> <tempo_processamento_ms> [porta]
 * Ex:  ./compra 0.7 80
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

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Uso: %s <taxa_sucesso> <tempo_ms> [porta]\n", argv[0]);
        fprintf(stderr, "Ex:  %s 0.7 80\n", argv[0]);
        return 1;
    }

    double taxa_sucesso        = atof(argv[1]);
    int    tempo_processamento = atoi(argv[2]);
    int    porta               = (argc >= 4) ? atoi(argv[3]) : PORTA_COMPRA;

    srand((unsigned int)(time(NULL) ^ getpid()));

    /* -----------------------------------------------------------------------
     * [Request-Reply] Configuração do socket servidor
     * ----------------------------------------------------------------------- */
    int fd_servidor = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_servidor < 0) {
        perror("[COMPRA] Erro ao criar socket");
        return 1;
    }

    int opcao = 1;
    setsockopt(fd_servidor, SOL_SOCKET, SO_REUSEADDR, &opcao, sizeof(opcao));

    struct sockaddr_in endereco;
    memset(&endereco, 0, sizeof(endereco));
    endereco.sin_family      = AF_INET;
    endereco.sin_addr.s_addr = INADDR_ANY;
    endereco.sin_port        = htons((uint16_t)porta);

    if (bind(fd_servidor, (struct sockaddr *)&endereco, sizeof(endereco)) < 0) {
        perror("[COMPRA] Erro no bind");
        close(fd_servidor);
        return 1;
    }

    if (listen(fd_servidor, BACKLOG) < 0) {
        perror("[COMPRA] Erro no listen");
        close(fd_servidor);
        return 1;
    }

    printf("[COMPRA] Aguardando conexões na porta %d "
           "(taxa=%.0f%%, delay=%dms)\n",
           porta, taxa_sucesso * 100, tempo_processamento);
    fflush(stdout);

    /* -----------------------------------------------------------------------
     * [Request-Reply] Loop principal de atendimento
     * ----------------------------------------------------------------------- */
    while (1) {
        struct sockaddr_in cliente;
        socklen_t tamanho_cliente = sizeof(cliente);

        int fd_cliente = accept(fd_servidor, (struct sockaddr *)&cliente, &tamanho_cliente);
        if (fd_cliente < 0) {
            perror("[COMPRA] Erro no accept");
            continue;
        }

        /* [Request-Reply] Recebe a ordem de compra ou venda */
        MsgOrdemCompra ordem;
        ssize_t recebido = recv(fd_cliente, &ordem, sizeof(ordem), MSG_WAITALL);
        if (recebido <= 0) {
            perror("[COMPRA] Erro ao receber ordem");
            close(fd_cliente);
            continue;
        }

        const char *tipo_str = (ordem.tipo == OP_COMPRA) ? "COMPRA" : "VENDA";
        char buffer_log[256];
        snprintf(buffer_log, sizeof(buffer_log),
                 "Ordem recebida: %s %s @ %.4f",
                 tipo_str, ordem.par, ordem.preco);
        log_evento("COMPRA", buffer_log);

        /* Simula tempo de execução na exchange */
        struct timespec espera = { 0, (long)tempo_processamento * 1000000L };
        nanosleep(&espera, NULL);

        MsgRespostaCompra resposta;
        memset(&resposta, 0, sizeof(resposta));

        if (ordem.tipo == OP_VENDA) {
            /*
             * [Saga] Transação compensatória: vendas sempre retornam sucesso.
             * Simplificação válida do protótipo — compensações não podem falhar
             * de forma definitiva; em caso de falha real, seriam reenviadas.
             */
            resposta.status = STATUS_SUCESSO;
            snprintf(buffer_log, sizeof(buffer_log),
                     "VENDA compensatória executada: %s @ %.4f — SUCESSO",
                     ordem.par, ordem.preco);
            log_evento("COMPRA", buffer_log);
        } else {
            /* OP_COMPRA: sorteia sucesso/falha conforme taxa configurada */
            double sorteio = (double)rand() / RAND_MAX;
            if (sorteio < taxa_sucesso) {
                resposta.status = STATUS_SUCESSO;
                snprintf(buffer_log, sizeof(buffer_log),
                         "COMPRA executada: %s @ %.4f — SUCESSO",
                         ordem.par, ordem.preco);
            } else {
                resposta.status = STATUS_FALHA;
                snprintf(buffer_log, sizeof(buffer_log),
                         "COMPRA recusada: %s @ %.4f — FALHA (liquidez insuficiente)",
                         ordem.par, ordem.preco);
            }
            log_evento("COMPRA", buffer_log);
        }

        /* [Request-Reply] Envia a resposta e encerra a conexão */
        ssize_t enviado = send(fd_cliente, &resposta, sizeof(resposta), 0);
        if (enviado < 0) {
            perror("[COMPRA] Erro ao enviar resposta");
        }

        close(fd_cliente);
    }

    close(fd_servidor);
    return 0;
}
