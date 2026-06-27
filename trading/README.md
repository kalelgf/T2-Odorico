# Sistema de Trading Distribuído em C

Sistema de trading automatizado distribuído implementado com Berkeley Sockets TCP em C, composto por 4 processos independentes que se comunicam via rede aplicando 3 padrões de projeto de programação distribuída.

---

## Padrões de Projeto Implementados

### Padrão 1 — Saga (Orquestração)

O processo `operacao.c` é o orquestrador central da Saga. Ele mantém explicitamente o estado da transação distribuída no enum `EstadoSaga` (definido em `protocolo.h`) e executa as etapas em sequência: cotação → análise de risco → compra do ETH/USDT → compra do USD/BRL. A **aprovação do risco** é o **ponto pivô** — após ela, o sistema está comprometido a concluir ou compensar. Transações compensatórias (venda de ETH/USDT via `OP_VENDA`) são disparadas em dois casos: (a) TTL da cotação expira após a compra 1, e (b) a compra do USD/BRL falha. Cada transição de estado é logada com `[ORDEM-N][ESTADO]` para rastreabilidade completa. Implementado em `operacao.c`, funções `executar_saga()`, `compensar_compra_eth()`.

### Padrão 2 — Request-Reply

Cada uma das 4 integrações entre `operacao` e os serviços segue o padrão Request-Reply síncrono sobre TCP. O orquestrador abre uma conexão dedicada por chamada (`connect` → `send` → `recv` bloqueante → `close`), aguardando a resposta antes de prosseguir. Os servidores implementam o ciclo `socket` → `bind` → `listen` → loop de `accept` → `recv` → processa → `send` → `close`. A latência configurável de cada serviço (argumento `tempo_processamento_ms`) simula o "processamento assíncrono" descrito no padrão Azure Async Request-Reply. Implementado em todos os servidores (`cotacao.c`, `risco.c`, `compra.c`) e na função `conectar_servico()` de `operacao.c`.

### Padrão 3 — Message Expiration

O serviço `cotacao.c` (Sender) embute na mensagem de resposta `MsgCotacao` os campos `timestamp_emissao_ms` (capturado via `clock_gettime(CLOCK_MONOTONIC)` após o processamento) e `ttl_ms` (configurável via argumento). O orquestrador (Receiver) chama `cotacao_expirada()` em **3 pontos críticos**: (1) antes de consultar o serviço de risco, (2) antes da compra do ETH/USDT, e (3) antes da compra do USD/BRL. Cotação expirada equivale ao Dead Letter Channel do EIP — a ordem é descartada e registrada em log como `FALHA_TTL`. Implementado em `protocolo.h` (`cotacao_expirada()`) e `operacao.c` (`executar_saga()`).

---

## Compilação

```bash
make all
```

Requisitos: `gcc`, `make`, sistema POSIX (Linux/macOS). O Makefile compila os 4 binários: `operacao`, `cotacao`, `risco`, `compra`.

```bash
make clean   # remove os binários compilados
```

---

## Execução

Abra **4 terminais separados** e execute na seguinte ordem (serviços antes do orquestrador):

```bash
# Terminal 1 — Servidor de cotação
./cotacao 0.9 50 300
# taxa de sucesso 90%, delay 50ms, TTL da cotação 300ms

# Terminal 2 — Servidor de risco
./risco 0.9 80
# taxa de aprovação 90%, delay 80ms

# Terminal 3 — Servidor de compras
./compra 0.9 60
# taxa de sucesso 90%, delay 60ms

# Terminal 4 — Orquestrador (executar por último)
./operacao 5
# processa 5 ordens de trading
```

---

## Cenários de Demonstração

### Cenário (i) — Sucesso: todas as etapas completam sem falha

```bash
# Terminal 1
./cotacao 1.0 10 1000

# Terminal 2
./risco 1.0 10

# Terminal 3
./compra 1.0 10

# Terminal 4
./operacao 3
```

Resultado esperado: todas as 3 ordens terminam com estado `CONCLUIDA`.

---

### Cenário (ii) — Falha por TTL: cotação expira durante análise de risco

```bash
# Terminal 1 — TTL de apenas 50ms
./cotacao 1.0 10 50

# Terminal 2 — delay de 200ms (maior que o TTL de 50ms)
./risco 1.0 200

# Terminal 3
./compra 1.0 10

# Terminal 4
./operacao 3
```

Resultado esperado: o risco demora 200ms mas o TTL é 50ms, portanto a verificação 1 (antes do risco) ou 2 (antes da compra 1) falha. Ordens terminam com `FALHA_TTL`.

---

### Cenário (iii) — Compensação Saga: TTL expira após compra 1

Este cenário demonstra a **transação compensatória** da Saga: a compra do ETH/USDT é realizada com sucesso, mas a cotação expira antes da compra do USD/BRL. O orquestrador desfaz a compra 1 enviando uma ordem de venda.

```bash
# Terminal 1 — TTL de 100ms: expira durante o delay de 90ms da compra
./cotacao 1.0 5 100

# Terminal 2
./risco 1.0 5

# Terminal 3 — delay de 90ms: compra 1 ocorre ~90ms após timestamp, TTL expira antes da compra 2
./compra 1.0 90

# Terminal 4
./operacao 3
```

Resultado esperado: `COMPRA1_REALIZADA` → TTL expira → `FALHA_TTL` → VENDA compensatória do ETH/USDT → estado final `COMPENSADA`.

**Variante — Falha na compra 1 (sem compensação):**

```bash
# Terminal 1
./cotacao 1.0 10 5000

# Terminal 2
./risco 1.0 10

# Terminal 3 — 0% de sucesso: compra 1 falha, nenhuma compensação necessária
./compra 0.0 10

# Terminal 4
./operacao 3
```

Resultado esperado: `FALHA_COMPRA1` (sem compensação, pois a compra nunca foi executada).

---

## Argumentos dos Binários

| Binário    | Argumentos                                              |
|------------|---------------------------------------------------------|
| `operacao` | `<N_ordens> [host_cotacao] [host_risco] [host_compra]` |
| `cotacao`  | `<taxa_sucesso> <tempo_ms> <ttl_ms> [porta]`           |
| `risco`    | `<taxa_sucesso> <tempo_ms> [porta]`                    |
| `compra`   | `<taxa_sucesso> <tempo_ms> [porta]`                    |

- `taxa_sucesso`: valor entre 0.0 (0%) e 1.0 (100%)
- `tempo_ms`: latência simulada em milissegundos
- `ttl_ms`: tempo de vida da cotação em milissegundos (apenas `cotacao`)
