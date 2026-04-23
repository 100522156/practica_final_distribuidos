# Makefile - Servidor de mensajeria (Parte 1)
# Sistemas Distribuidos - UC3M

# compilador
CC = gcc

# flags: wall para avisos, g para depuracion, pthread para los hilos
CFLAGS = -Wall -g -pthread

# librerias necesarias: pthread para hilos concurrentes
LDLIBS = -lpthread

# ejecutable del servidor
SERVER = server

# regla principal
all: $(SERVER)

# compilacion del servidor
# depende unicamente de server.c
$(SERVER): server.c
	$(CC) $(CFLAGS) -o $@ server.c $(LDLIBS)

# limpieza de ficheros generados
clean:
	rm -f $(SERVER) *.o