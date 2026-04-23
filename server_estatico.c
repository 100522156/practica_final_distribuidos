/*
 * server.c - Servidor de mensajeria (Parte 1)
 * Sistemas Distribuidos - UC3M
 *
 * Servidor concurrente multihilo que gestiona:
 *  - Registro/baja de usuarios
 *  - Conexion/desconexion
 *  - Envio y almacenamiento de mensajes pendientes
 *  - Listado de usuarios conectados
 *
 * Protocolo: sockets TCP, cadenas terminadas en '\0', un byte de respuesta
 * Uso: ./server -p <puerto>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <signal.h>
#include <ifaddrs.h>

/* ===================== CONSTANTES ===================== */
#define MAX_USERS    100
#define MAX_MSGS     1000
#define MAX_NAME     256
#define MAX_MSG_TEXT 256
#define MAX_IP       64

/* ===================== ESTRUCTURAS ===================== */

/* Estado posible de un usuario */
typedef enum { DESCONECTADO, CONECTADO } EstadoUsuario;

/* Informacion de cada usuario registrado */
struct Usuario {
    char nombre[MAX_NAME];
    EstadoUsuario estado;
    char ip[MAX_IP];
    int  puerto;
    unsigned int ultimo_id; /* ultimo identificador de mensaje asignado al enviar */
};

/* Mensaje pendiente de entrega */
struct Mensaje {
    int           ocupado;            /* 1 si este slot esta en uso */
    char          destino[MAX_NAME];  /* usuario que debe recibirlo */
    char          remitente[MAX_NAME];/* usuario que lo envio */
    unsigned int  id;                 /* identificador del mensaje */
    char          texto[MAX_MSG_TEXT];/* contenido del mensaje */
};

/* ===================== ESTADO GLOBAL ===================== */
struct Usuario usuarios[MAX_USERS];
int num_usuarios = 0;

struct Mensaje mensajes[MAX_MSGS];

/* mutex para proteger acceso concurrente a usuarios y mensajes */
pthread_mutex_t mutex_usuarios = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_mensajes = PTHREAD_MUTEX_INITIALIZER;

/* ===================== FUNCIONES DE RED ===================== */

/* Envia todos los bytes del buffer, repitiendo si write devuelve menos de lo pedido */
int sendMessage(int socket, char *buffer, int len) {
    int r;
    int l = len;
    do {
        r = write(socket, buffer, l);
        l = l - r;
        buffer = buffer + r;
    } while ((l > 0) && (r >= 0));
    if (r < 0) return -1;
    return 0;
}

/* Lee exactamente len bytes del socket */
int recvMessage(int socket, char *buffer, int len) {
    int r;
    int l = len;
    do {
        r = read(socket, buffer, l);
        l = l - r;
        buffer = buffer + r;
    } while ((l > 0) && (r >= 0));
    if (r < 0) return -1;
    return 0;
}

/* Lee una linea del descriptor fd hasta encontrar '\n' o '\0' (igual que en servidor-sock.c) */
ssize_t readLine(int fd, void *buffer, size_t n) {
    ssize_t numRead;
    size_t  totRead;
    char   *buf;
    char    ch;

    if (n <= 0 || buffer == NULL) {
        errno = EINVAL;
        return -1;
    }
    buf      = buffer;
    totRead  = 0;

    for (;;) {
        numRead = read(fd, &ch, 1);
        if (numRead == -1) {
            if (errno == EINTR) continue;
            else return -1;
        } else if (numRead == 0) {
            if (totRead == 0) return 0;
            else break;
        } else {
            /* separadores de mensaje: \n o \0 */
            if (ch == '\n') break;
            if (ch == '\0') break;
            if (totRead < n - 1) {
                totRead++;
                *buf++ = ch;
            }
        }
    }
    *buf = '\0';
    return totRead;
}

/* Envia una cadena acabada en \n (nuestro separador en el protocolo de texto) */
int enviar_linea(int sock, const char *texto) {
    char buf[512];
    snprintf(buf, sizeof(buf), "%s\n", texto);
    return sendMessage(sock, buf, strlen(buf));
}

/* Envia un unico byte de resultado (codigos de respuesta del protocolo) */
int enviar_byte(int sock, unsigned char valor) {
    return sendMessage(sock, (char *)&valor, 1);
}

/* ===================== BUSQUEDA DE USUARIOS ===================== */

/* Devuelve el indice del usuario con ese nombre, o -1 si no existe */
int buscar_usuario(const char *nombre) {
    for (int i = 0; i < num_usuarios; i++) {
        if (strcmp(usuarios[i].nombre, nombre) == 0)
            return i;
    }
    return -1;
}

/* ===================== OBTENCION DE IP LOCAL ===================== */

/* Obtiene la IP local de la primera interfaz que no sea loopback */
void obtener_ip_local(char *ip_buf, size_t len) {
    struct ifaddrs *ifaddr, *ifa;
    if (getifaddrs(&ifaddr) == -1) {
        strncpy(ip_buf, "127.0.0.1", len);
        return;
    }
    strncpy(ip_buf, "127.0.0.1", len);
    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL) continue;
        if (ifa->ifa_addr->sa_family != AF_INET) continue;
        /* ignoramos loopback */
        struct sockaddr_in *sa = (struct sockaddr_in *)ifa->ifa_addr;
        char tmp[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &sa->sin_addr, tmp, sizeof(tmp));
        if (strcmp(tmp, "127.0.0.1") != 0) {
            strncpy(ip_buf, tmp, len);
            break;
        }
    }
    freeifaddrs(ifaddr);
}

/* ===================== ENVIO DE MENSAJES PENDIENTES ===================== */

/*
 * Envia un mensaje a un cliente conectado siguiendo el protocolo 8.6:
 *   1. conectar al thread de escucha del cliente
 *   2. enviar "SEND_MESSAGE"
 *   3. enviar remitente
 *   4. enviar id (como cadena)
 *   5. enviar texto del mensaje
 *   6. cerrar conexion
 * Devuelve 0 si se entrego con exito, -1 si fallo.
 */
int enviar_mensaje_a_cliente(const char *ip, int puerto,
                              const char *remitente,
                              unsigned int id,
                              const char *texto) {
    struct sockaddr_in addr;
    int sock;
    char buf[64];

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(puerto);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    /* protocolo sección 8.6 */
    enviar_linea(sock, "SEND_MESSAGE");
    enviar_linea(sock, remitente);
    snprintf(buf, sizeof(buf), "%u", id);
    enviar_linea(sock, buf);
    enviar_linea(sock, texto);

    close(sock);
    return 0;
}

/*
 * Envia el ACK al remitente del mensaje (protocolo 8.6, parte 2):
 *   1. conectar al thread de escucha del remitente
 *   2. enviar "SEND_MESS_ACK"
 *   3. enviar id (como cadena)
 *   4. cerrar conexion
 * Si el remitente no esta conectado se descarta (segun el enunciado).
 */
int enviar_ack_remitente(int idx_remitente, unsigned int id) {
    char buf[64];
    int  sock;
    struct sockaddr_in addr;

    /* segun el enunciado: si el remitente no esta conectado, se descarta */
    if (usuarios[idx_remitente].estado != CONECTADO) return 0;

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(usuarios[idx_remitente].puerto);
    if (inet_pton(AF_INET, usuarios[idx_remitente].ip, &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    enviar_linea(sock, "SEND_MESS_ACK");
    snprintf(buf, sizeof(buf), "%u", id);
    enviar_linea(sock, buf);

    close(sock);
    return 0;
}

/*
 * Intenta entregar todos los mensajes pendientes del usuario idx_destino.
 * Se llama justo despues de que el usuario se conecta (seccion 7.4).
 * Para cada mensaje pendiente:
 *   - lo envia al cliente
 *   - si se entrega con exito: notifica al remitente y borra el mensaje
 *   - si falla: marca al destino como desconectado y para
 */
void enviar_pendientes(int idx_destino) {
    /* recorremos todos los slots de mensajes */
    for (int i = 0; i < MAX_MSGS; i++) {
        pthread_mutex_lock(&mutex_mensajes);
        if (!mensajes[i].ocupado ||
            strcmp(mensajes[i].destino, usuarios[idx_destino].nombre) != 0) {
            pthread_mutex_unlock(&mutex_mensajes);
            continue;
        }

        /* copiamos los datos para no mantener el mutex durante el envio */
        char          rem[MAX_NAME], dest[MAX_NAME], texto[MAX_MSG_TEXT];
        unsigned int  id = mensajes[i].id;
        strncpy(rem,   mensajes[i].remitente, MAX_NAME - 1);
        strncpy(dest,  mensajes[i].destino,   MAX_NAME - 1);
        strncpy(texto, mensajes[i].texto,      MAX_MSG_TEXT - 1);
        pthread_mutex_unlock(&mutex_mensajes);

        /* necesitamos la IP/puerto del destino y el indice del remitente */
        pthread_mutex_lock(&mutex_usuarios);
        char ip_dest[MAX_IP];
        int  puerto_dest = usuarios[idx_destino].puerto;
        strncpy(ip_dest, usuarios[idx_destino].ip, MAX_IP - 1);
        int idx_rem = buscar_usuario(rem);
        pthread_mutex_unlock(&mutex_usuarios);

        int ok = enviar_mensaje_a_cliente(ip_dest, puerto_dest, rem, id, texto);

        if (ok == 0) {
            /* entregado: mostramos log, borramos el mensaje y notificamos al remitente */
            printf("s> SEND MESSAGE %u FROM %s TO %s\n", id, rem, dest);
            fflush(stdout);

            pthread_mutex_lock(&mutex_mensajes);
            mensajes[i].ocupado = 0;
            pthread_mutex_unlock(&mutex_mensajes);

            if (idx_rem >= 0) {
                pthread_mutex_lock(&mutex_usuarios);
                enviar_ack_remitente(idx_rem, id);
                pthread_mutex_unlock(&mutex_usuarios);
            }
        } else {
            /* fallo al entregar: marcamos al destino como desconectado y paramos */
            pthread_mutex_lock(&mutex_usuarios);
            usuarios[idx_destino].estado = DESCONECTADO;
            memset(usuarios[idx_destino].ip, 0, MAX_IP);
            usuarios[idx_destino].puerto = 0;
            pthread_mutex_unlock(&mutex_usuarios);
            break;
        }
    }
}

/* ===================== PROCESADO DE OPERACIONES ===================== */

/* --- REGISTER --- */
void op_register(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 2);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);

    if (buscar_usuario(nombre) >= 0) {
        /* ya existe: codigo 1 */
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> REGISTER %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    if (num_usuarios >= MAX_USERS) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 2);
        printf("s> REGISTER %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    /* guardamos el nuevo usuario */
    strncpy(usuarios[num_usuarios].nombre, nombre, MAX_NAME - 1);
    usuarios[num_usuarios].estado    = DESCONECTADO;
    usuarios[num_usuarios].puerto    = 0;
    usuarios[num_usuarios].ultimo_id = 0;
    memset(usuarios[num_usuarios].ip, 0, MAX_IP);
    num_usuarios++;

    pthread_mutex_unlock(&mutex_usuarios);

    enviar_byte(sock, 0);
    printf("s> REGISTER %s OK\n", nombre);
    fflush(stdout);
}

/* --- UNREGISTER --- */
void op_unregister(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 2);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);
    int idx = buscar_usuario(nombre);
    if (idx < 0) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> UNREGISTER %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    /* borramos mensajes pendientes de ese usuario */
    pthread_mutex_lock(&mutex_mensajes);
    for (int i = 0; i < MAX_MSGS; i++) {
        if (mensajes[i].ocupado &&
            strcmp(mensajes[i].destino, nombre) == 0) {
            mensajes[i].ocupado = 0;
        }
    }
    pthread_mutex_unlock(&mutex_mensajes);

    /* eliminamos al usuario desplazando el array */
    for (int i = idx; i < num_usuarios - 1; i++)
        usuarios[i] = usuarios[i + 1];
    num_usuarios--;

    pthread_mutex_unlock(&mutex_usuarios);

    enviar_byte(sock, 0);
    printf("s> UNREGISTER %s OK\n", nombre);
    fflush(stdout);
}

/* --- CONNECT --- */
void op_connect(int sock, const char *ip_cliente) {
    char nombre[MAX_NAME];
    char puerto_str[32];

    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 3);
        return;
    }
    if (readLine(sock, puerto_str, sizeof(puerto_str)) <= 0) {
        enviar_byte(sock, 3);
        return;
    }

    int puerto = atoi(puerto_str);

    pthread_mutex_lock(&mutex_usuarios);
    int idx = buscar_usuario(nombre);

    if (idx < 0) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> CONNECT %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    if (usuarios[idx].estado == CONECTADO) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 2);
        printf("s> CONNECT %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    /* actualizamos IP, puerto y estado */
    strncpy(usuarios[idx].ip, ip_cliente, MAX_IP - 1);
    usuarios[idx].puerto = puerto;
    usuarios[idx].estado = CONECTADO;

    pthread_mutex_unlock(&mutex_usuarios);

    enviar_byte(sock, 0);
    printf("s> CONNECT %s OK\n", nombre);
    fflush(stdout);

    /* enviamos los mensajes pendientes en un hilo separado para no bloquear */
    enviar_pendientes(idx);
}

/* --- DISCONNECT --- */
void op_disconnect(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 3);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);
    int idx = buscar_usuario(nombre);

    if (idx < 0) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> DISCONNECT %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    if (usuarios[idx].estado != CONECTADO) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 2);
        printf("s> DISCONNECT %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    /* limpiamos IP, puerto y cambiamos estado */
    memset(usuarios[idx].ip, 0, MAX_IP);
    usuarios[idx].puerto = 0;
    usuarios[idx].estado = DESCONECTADO;

    pthread_mutex_unlock(&mutex_usuarios);

    enviar_byte(sock, 0);
    printf("s> DISCONNECT %s OK\n", nombre);
    fflush(stdout);
}

/* --- SEND --- */
void op_send(int sock) {
    char remitente[MAX_NAME];
    char destino[MAX_NAME];
    char texto[MAX_MSG_TEXT];

    if (readLine(sock, remitente, MAX_NAME)    <= 0) { enviar_byte(sock, 2); return; }
    if (readLine(sock, destino,   MAX_NAME)    <= 0) { enviar_byte(sock, 2); return; }
    if (readLine(sock, texto,     MAX_MSG_TEXT) < 0) { enviar_byte(sock, 2); return; }

    pthread_mutex_lock(&mutex_usuarios);

    int idx_rem  = buscar_usuario(remitente);
    int idx_dest = buscar_usuario(destino);

    /* si alguno de los dos no existe: error 1 */
    if (idx_rem < 0 || idx_dest < 0) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        return;
    }

    /* asignamos un identificador al mensaje (segun enunciado: unsigned int, empieza en 1) */
    usuarios[idx_rem].ultimo_id++;
    if (usuarios[idx_rem].ultimo_id == 0)
        usuarios[idx_rem].ultimo_id = 1; /* al desbordarse vuelve a 1 */
    unsigned int id = usuarios[idx_rem].ultimo_id;

    /* buscamos un slot libre para el mensaje */
    pthread_mutex_lock(&mutex_mensajes);
    int slot = -1;
    for (int i = 0; i < MAX_MSGS; i++) {
        if (!mensajes[i].ocupado) { slot = i; break; }
    }

    if (slot < 0) {
        /* no hay espacio: error 2 */
        pthread_mutex_unlock(&mutex_mensajes);
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 2);
        return;
    }

    /* guardamos el mensaje en el slot */
    mensajes[slot].ocupado = 1;
    strncpy(mensajes[slot].remitente, remitente, MAX_NAME - 1);
    strncpy(mensajes[slot].destino,   destino,   MAX_NAME - 1);
    strncpy(mensajes[slot].texto,     texto,     MAX_MSG_TEXT - 1);
    mensajes[slot].id = id;
    pthread_mutex_unlock(&mutex_mensajes);

    /* respondemos al remitente con exito + identificador */
    char id_str[32];
    snprintf(id_str, sizeof(id_str), "%u", id);
    enviar_byte(sock, 0);
    enviar_linea(sock, id_str);

    /* si el destino esta conectado intentamos entrega inmediata */
    if (usuarios[idx_dest].estado == CONECTADO) {
        char ip_dest[MAX_IP];
        int  puerto_dest = usuarios[idx_dest].puerto;
        strncpy(ip_dest, usuarios[idx_dest].ip, MAX_IP - 1);
        pthread_mutex_unlock(&mutex_usuarios);

        int ok = enviar_mensaje_a_cliente(ip_dest, puerto_dest,
                                           remitente, id, texto);
        if (ok == 0) {
            /* mensaje entregado: borramos del almacen y notificamos al remitente */
            printf("s> SEND MESSAGE %u FROM %s TO %s\n", id, remitente, destino);
            fflush(stdout);

            pthread_mutex_lock(&mutex_mensajes);
            mensajes[slot].ocupado = 0;
            pthread_mutex_unlock(&mutex_mensajes);

            pthread_mutex_lock(&mutex_usuarios);
            int idx_r2 = buscar_usuario(remitente);
            if (idx_r2 >= 0) enviar_ack_remitente(idx_r2, id);
            pthread_mutex_unlock(&mutex_usuarios);
        } else {
            /* fallo de envio: marcamos al destino como desconectado */
            pthread_mutex_lock(&mutex_usuarios);
            int idx_d2 = buscar_usuario(destino);
            if (idx_d2 >= 0) {
                usuarios[idx_d2].estado = DESCONECTADO;
                memset(usuarios[idx_d2].ip, 0, MAX_IP);
                usuarios[idx_d2].puerto = 0;
            }
            pthread_mutex_unlock(&mutex_usuarios);
            printf("s> MESSAGE %u FROM %s TO %s STORED\n", id, remitente, destino);
            fflush(stdout);
        }
    } else {
        /* destino desconectado: el mensaje queda almacenado */
        pthread_mutex_unlock(&mutex_usuarios);
        printf("s> MESSAGE %u FROM %s TO %s STORED\n", id, remitente, destino);
        fflush(stdout);
    }
}

/* --- USERS --- */
void op_users(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 2);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);

    int idx = buscar_usuario(nombre);
    if (idx < 0) {
        /* segun el enunciado seccion 7.7: si no esta registrado -> error tipo 2 */
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 2);
        printf("s> CONNECTEDUSERS FAIL\n");
        fflush(stdout);
        return;
    }

    if (usuarios[idx].estado != CONECTADO) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> CONNECTEDUSERS FAIL\n");
        fflush(stdout);
        return;
    }

    /* contamos los conectados y guardamos sus nombres */
    char conectados[MAX_USERS][MAX_NAME];
    int  n = 0;
    for (int i = 0; i < num_usuarios; i++) {
        if (usuarios[i].estado == CONECTADO) {
            strncpy(conectados[n], usuarios[i].nombre, MAX_NAME - 1);
            n++;
        }
    }
    pthread_mutex_unlock(&mutex_usuarios);

    /* enviamos: byte 0, numero de conectados (cadena), y luego cada nombre */
    char buf[32];
    enviar_byte(sock, 0);
    snprintf(buf, sizeof(buf), "%d", n);
    enviar_linea(sock, buf);

    for (int i = 0; i < n; i++)
        enviar_linea(sock, conectados[i]);

    printf("s> CONNECTEDUSERS OK\n");
    fflush(stdout);
}

/* ===================== HILO POR CLIENTE ===================== */

/*
 * Funcion que ejecuta cada hilo.
 * Recibe el socket de la conexion aceptada y la IP del cliente.
 * Lee la operacion y despacha al handler correspondiente.
 */
void *procesar_cliente(void *arg) {
    /* desempaquetamos socket e IP del cliente */
    int  *datos = (int *)arg;
    int   sock  = datos[0];
    /* la IP del cliente se pasa como entero (en realidad un puntero a char)
       pero para simplificar la pasamos dentro de una estructura auxiliar */
    free(arg);

    /* leemos la operacion */
    char op[32];
    if (readLine(sock, op, sizeof(op)) <= 0) {
        close(sock);
        return NULL;
    }

    /* no tenemos la IP aqui; la obtenemos con getpeername */
    struct sockaddr_in peer;
    socklen_t peer_len = sizeof(peer);
    char ip_cliente[MAX_IP] = "0.0.0.0";
    if (getpeername(sock, (struct sockaddr *)&peer, &peer_len) == 0)
        inet_ntop(AF_INET, &peer.sin_addr, ip_cliente, sizeof(ip_cliente));

    /* despachamos segun la operacion */
    if      (strcmp(op, "REGISTER")   == 0) op_register(sock);
    else if (strcmp(op, "UNREGISTER") == 0) op_unregister(sock);
    else if (strcmp(op, "CONNECT")    == 0) op_connect(sock, ip_cliente);
    else if (strcmp(op, "DISCONNECT") == 0) op_disconnect(sock);
    else if (strcmp(op, "SEND")       == 0) op_send(sock);
    else if (strcmp(op, "USERS")      == 0) op_users(sock);
    else enviar_byte(sock, 2);

    close(sock);
    return NULL;
}

/* ===================== MAIN ===================== */

int main(int argc, char *argv[]) {
    int    port   = -1;

    /* parseamos los argumentos: ./server -p <puerto> */
    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "-p") == 0) {
            port = atoi(argv[i + 1]);
            break;
        }
    }
    if (port < 0) {
        fprintf(stderr, "Uso: %s -p <puerto>\n", argv[0]);
        return 1;
    }

    /* ignoramos SIGPIPE para que un write a un socket cerrado no mate al proceso */
    signal(SIGPIPE, SIG_IGN);

    /* inicializamos el array de mensajes a vacio */
    memset(mensajes, 0, sizeof(mensajes));

    /* creamos el socket del servidor */
    int servidor = socket(AF_INET, SOCK_STREAM, 0);
    if (servidor < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(servidor, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (bind(servidor, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(servidor, 10) < 0) {
        perror("listen"); return 1;
    }

    /* mostramos el mensaje de inicio con la IP local */
    char ip_local[MAX_IP];
    obtener_ip_local(ip_local, sizeof(ip_local));
    printf("s> init server %s:%d\n", ip_local, port);
    printf("s> \n");
    fflush(stdout);

    /* bucle principal: aceptamos conexiones y lanzamos un hilo por cada una */
    while (1) {
        struct sockaddr_in cliente_addr;
        socklen_t len = sizeof(cliente_addr);

        /* reservamos memoria para el descriptor del socket del cliente
           para que cada hilo tenga su propia copia (igual que en servidor-sock.c) */
        int *cliente_sock = malloc(sizeof(int));
        if (!cliente_sock) continue;

        *cliente_sock = accept(servidor, (struct sockaddr *)&cliente_addr, &len);
        if (*cliente_sock < 0) {
            free(cliente_sock);
            continue;
        }

        pthread_t hilo;
        pthread_create(&hilo, NULL, procesar_cliente, cliente_sock);
        /* detach para que el hilo libere sus recursos al terminar sin join */
        pthread_detach(hilo);
    }

    close(servidor);
    return 0;
}