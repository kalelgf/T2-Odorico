/*
 * cotacao.c — Servidor de Cotação (porta 8001)
 *
 * [Message Expiration — papel de Sender/Remetente]
 * Este serviço é o REMETENTE no padrão Message Expiration (EIP).
 * Ele embute na resposta os campos timestamp_emissao_ms e ttl_ms,
 * permitindo que o receptor (operacao.c) valide a expiração da mensagem
 * antes de prosseguir com cada etapa da Saga.
 *
 * [Request-Reply — papel de Replier]
 * Implementa o lado servidor do padrão: socket→bind→listen→accept→
 * recv(req)→processa→send(reply)→close, em loop infinito.
 *
 * Uso: ./cotacao <taxa_sucesso> <tempo_processamento_ms> <ttl_cotacao_ms> [porta]
 * Ex:  ./cotacao 0.9 50 300
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

/* Gera um double aleatório no intervalo [min, max] */
static double aleatorio_entre(double min, double max) {
    return min + ((double)rand() / RAND_MAX) * (max - min);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Uso: %s <taxa_sucesso> <tempo_ms> <ttl_ms> [porta]\n", argv[0]);
        fprintf(stderr, "Ex:  %s 0.9 50 300\n", argv[0]);
        return 1;
    }

    double taxa_sucesso         = atof(argv[1]);
    int    tempo_processamento  = atoi(argv[2]);
    int    ttl_cotacao_ms       = atoi(argv[3]);
    int    porta                = (argc >= 5) ? atoi(argv[4]) : PORTA_COTACAO;

    /* Semente independente por processo para aleatoriedade real */
    srand((unsigned int)(time(NULL) ^ getpid()));

    /* -----------------------------------------------------------------------
     * [Request-Reply] Configuração do socket servidor
     * ----------------------------------------------------------------------- */
    int fd_servidor = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_servidor < 0) {
        perror("[COTACAO] Erro ao criar socket");
        return 1;
    }

    /* Permite reutilizar a porta imediatamente após reinicialização */
    int opcao = 1;
    setsockopt(fd_servidor, SOL_SOCKET, SO_REUSEADDR, &opcao, sizeof(opcao));

    struct sockaddr_in endereco;
    memset(&endereco, 0, sizeof(endereco));
    endereco.sin_family      = AF_INET;
    endereco.sin_addr.s_addr = INADDR_ANY;
    endereco.sin_port        = htons((uint16_t)porta);

    if (bind(fd_servidor, (struct sockaddr *)&endereco, sizeof(endereco)) < 0) {
        perror("[COTACAO] Erro no bind");
        close(fd_servidor);
        return 1;
    }

    if (listen(fd_servidor, BACKLOG) < 0) {
        perror("[COTACAO] Erro no listen");
        close(fd_servidor);
        return 1;
    }

    printf("[COTACAO] Aguardando conexões na porta %d "
           "(taxa=%.0f%%, delay=%dms, ttl=%dms)\n",
           porta, taxa_sucesso * 100, tempo_processamento, ttl_cotacao_ms);
    fflush(stdout);

    /* -----------------------------------------------------------------------
     * [Request-Reply] Loop principal de atendimento
     * ----------------------------------------------------------------------- */
    while (1) {
        struct sockaddr_in cliente;
        socklen_t tamanho_cliente = sizeof(cliente);

        int fd_cliente = accept(fd_servidor, (struct sockaddr *)&cliente, &tamanho_cliente);
        if (fd_cliente < 0) {
            perror("[COTACAO] Erro no accept");
            continue;
        }

        char buffer_log[128];
        snprintf(buffer_log, sizeof(buffer_log),
                 "Requisição recebida de %s:%d",
                 inet_ntoa(cliente.sin_addr), ntohs(cliente.sin_port));
        log_evento("COTACAO", buffer_log);

        /* Simula latência de processamento (ex: consulta a exchange externa) */
        struct timespec espera = { 0, (long)tempo_processamento * 1000000L };
        nanosleep(&espera, NULL);

        /* -----------------------------------------------------------------------
         * [Message Expiration — Sender] Preenche a resposta com TTL e timestamp.
         * O timestamp é capturado APÓS o processamento, representando o momento
         * real em que a cotação foi gerada e está pronta para entrega.
         * ----------------------------------------------------------------------- */
        MsgCotacao resposta;
        memset(&resposta, 0, sizeof(resposta));

        double sorteio = (double)rand() / RAND_MAX;
        if (sorteio < taxa_sucesso) {
            resposta.status              = STATUS_SUCESSO;
            resposta.preco_eth_usdt      = aleatorio_entre(1000.0, 3000.0);
            resposta.preco_usd_brl       = aleatorio_entre(4.5, 6.0);
            resposta.timestamp_emissao_ms = tempo_atual_ms(); /* [Message Expiration] momento exato de geração */
            resposta.ttl_ms              = ttl_cotacao_ms;

            snprintf(buffer_log, sizeof(buffer_log),
                     "Cotação gerada: ETH/USDT=%.2f, USD/BRL=%.4f, TTL=%dms",
                     resposta.preco_eth_usdt, resposta.preco_usd_brl, ttl_cotacao_ms);
            log_evento("COTACAO", buffer_log);
        } else {
            resposta.status = STATUS_FALHA;
            resposta.ttl_ms = 0;
            log_evento("COTACAO", "Falha simulada: serviço indisponível");
        }

        /* [Request-Reply] Envia a resposta e encerra a conexão */
        ssize_t enviado = send(fd_cliente, &resposta, sizeof(resposta), 0);
        if (enviado < 0) {
            perror("[COTACAO] Erro ao enviar resposta");
        }

        close(fd_cliente);
    }

    close(fd_servidor);
    return 0;
}
