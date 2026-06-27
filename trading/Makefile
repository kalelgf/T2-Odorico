CC     = gcc
CFLAGS = -Wall -Wextra -g
TARGETS = operacao cotacao risco compra

all: $(TARGETS)

operacao: operacao.c protocolo.h
	$(CC) $(CFLAGS) -o operacao operacao.c

cotacao: cotacao.c protocolo.h
	$(CC) $(CFLAGS) -o cotacao cotacao.c

risco: risco.c protocolo.h
	$(CC) $(CFLAGS) -o risco risco.c

compra: compra.c protocolo.h
	$(CC) $(CFLAGS) -o compra compra.c

clean:
	rm -f $(TARGETS)

.PHONY: all clean
