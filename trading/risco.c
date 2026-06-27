/*
 * risco.c — Servidor de Análise de Risco (porta 8002)
 *
 * [Request-Reply — papel de Replier]
 * Implementa o padrão Request-Reply: recebe MsgRequisicaoRisco,
 * analisa os preços da cotação, retorna MsgRespostaRisco com a decisão.
 * Cada requisição é atendida em uma conexão TCP independente.
 *
 * Uso: ./risco <taxa_sucesso> <tempo_processamento_ms> [porta]
 * Ex:  ./risco 0.8 100
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
        fprintf(stderr, "Ex:  %s 0.8 100\n", argv[0]);
        return 1;
    }

    double taxa_sucesso        = atof(argv[1]);
    int    tempo_processamento = atoi(argv[2]);
    int    porta               = (argc >= 4) ? atoi(argv[3]) : PORTA_RISCO;

    srand((unsigned int)(time(NULL) ^ getpid()));

    /* -----------------------------------------------------------------------
     * [Request-Reply] Configuração do socket servidor
     * ----------------------------------------------------------------------- */
    int fd_servidor = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_servidor < 0) {
        perror("[RISCO] Erro ao criar socket");
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
        perror("[RISCO] Erro no bind");
        close(fd_servidor);
        return 1;
    }

    if (listen(fd_servidor, BACKLOG) < 0) {
        perror("[RISCO] Erro no listen");
        close(fd_servidor);
        return 1;
    }

    printf("[RISCO] Aguardando conexões na porta %d "
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
            perror("[RISCO] Erro no accept");
            continue;
        }

        /* [Request-Reply] Recebe a requisição com a cotação completa */
        MsgRequisicaoRisco requisicao;
        ssize_t recebido = recv(fd_cliente, &requisicao, sizeof(requisicao), MSG_WAITALL);
        if (recebido <= 0) {
            perror("[RISCO] Erro ao receber requisição");
            close(fd_cliente);
            continue;
        }

        char buffer_log[256];
        snprintf(buffer_log, sizeof(buffer_log),
                 "Análise solicitada: ETH/USDT=%.2f, USD/BRL=%.4f",
                 requisicao.cotacao.preco_eth_usdt,
                 requisicao.cotacao.preco_usd_brl);
        log_evento("RISCO", buffer_log);

        /* Simula tempo de análise (processamento de modelos de risco) */
        struct timespec espera = { 0, (long)tempo_processamento * 1000000L };
        nanosleep(&espera, NULL);

        MsgRespostaRisco resposta;
        memset(&resposta, 0, sizeof(resposta));

        double sorteio = (double)rand() / RAND_MAX;
        if (sorteio < taxa_sucesso) {
            resposta.status = STATUS_SUCESSO;
            log_evento("RISCO", "Decisão: APROVADO");
        } else {
            resposta.status = STATUS_FALHA;
            log_evento("RISCO", "Decisão: REPROVADO (risco excessivo)");
        }

        /* [Request-Reply] Envia a decisão e encerra a conexão */
        ssize_t enviado = send(fd_cliente, &resposta, sizeof(resposta), 0);
        if (enviado < 0) {
            perror("[RISCO] Erro ao enviar resposta");
        }

        close(fd_cliente);
    }

    close(fd_servidor);
    return 0;
}
