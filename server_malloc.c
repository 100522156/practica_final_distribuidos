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
 * Almacenamiento: listas enlazadas dinamicas con malloc/free
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
#define MAX_NAME     256
#define MAX_MSG_TEXT 256
#define MAX_IP       64

/* ===================== ESTRUCTURAS ===================== */

/* Estado posible de un usuario */
typedef enum { DESCONECTADO, CONECTADO } EstadoUsuario;

/*
 * Nodo de la lista enlazada de usuarios.
 * Cada nodo contiene la informacion de un usuario registrado
 * y un puntero al siguiente nodo.
 */
typedef struct NodoUsuario {
    char              nombre[MAX_NAME];
    EstadoUsuario     estado;
    char              ip[MAX_IP];
    int               puerto;
    unsigned int      ultimo_id; /* ultimo identificador de mensaje asignado */
    struct NodoUsuario *siguiente;
} NodoUsuario;

/*
 * Nodo de la lista enlazada de mensajes pendientes.
 * Cada nodo almacena un mensaje pendiente de entrega.
 */
typedef struct NodoMensaje {
    char             destino[MAX_NAME];   /* usuario destinatario */
    char             remitente[MAX_NAME]; /* usuario que lo envio */
    unsigned int     id;                  /* identificador del mensaje */
    char             texto[MAX_MSG_TEXT]; /* contenido del mensaje */
    struct NodoMensaje *siguiente;
} NodoMensaje;

/* ===================== ESTADO GLOBAL ===================== */

/* Cabeceras de las listas enlazadas */
NodoUsuario *lista_usuarios = NULL;
NodoMensaje *lista_mensajes = NULL;

/* mutex para proteger acceso concurrente a las listas */
pthread_mutex_t mutex_usuarios = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_mensajes = PTHREAD_MUTEX_INITIALIZER;

/* ===================== FUNCIONES DE RED ===================== */

/*
 * Envia todos los bytes del buffer,
 * repitiendo si write devuelve menos de lo pedido.
 */
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

/*
 * Lee una linea del descriptor fd hasta encontrar '\n' o '\0'.
 * Almacena la cadena en buffer (sin el terminador) y devuelve
 * el numero de bytes leidos, o -1 en caso de error.
 */
ssize_t readLine(int fd, void *buffer, size_t n) {
    ssize_t numRead;
    size_t  totRead;
    char   *buf;
    char    ch;

    if (n <= 0 || buffer == NULL) {
        errno = EINVAL;
        return -1;
    }
    buf     = buffer;
    totRead = 0;

    for (;;) {
        numRead = read(fd, &ch, 1);
        if (numRead == -1) {
            if (errno == EINTR) continue;
            else return -1;
        } else if (numRead == 0) {
            if (totRead == 0) return 0;
            else break;
        } else {
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

/* Envia una cadena acabada en '\n' (separador en el protocolo de texto) */
int enviar_linea(int sock, const char *texto) {
    char buf[512];
    snprintf(buf, sizeof(buf), "%s\n", texto);
    return sendMessage(sock, buf, strlen(buf));
}

/* Envia un unico byte de resultado (codigos de respuesta del protocolo) */
int enviar_byte(int sock, unsigned char valor) {
    return sendMessage(sock, (char *)&valor, 1);
}

/* ===================== GESTION DE LA LISTA DE USUARIOS ===================== */

/*
 * Busca un usuario por nombre en la lista enlazada.
 * Devuelve el puntero al nodo o NULL si no existe.
 * PRECONDICION: debe llamarse con mutex_usuarios tomado.
 */
NodoUsuario *buscar_usuario(const char *nombre) {
    NodoUsuario *actual = lista_usuarios;
    while (actual != NULL) {
        if (strcmp(actual->nombre, nombre) == 0)
            return actual;
        actual = actual->siguiente;
    }
    return NULL;
}

/*
 * Crea un nuevo nodo de usuario e inserta al inicio de la lista.
 * Devuelve el puntero al nuevo nodo, o NULL si falla el malloc.
 * PRECONDICION: debe llamarse con mutex_usuarios tomado.
 */
NodoUsuario *insertar_usuario(const char *nombre) {
    NodoUsuario *nuevo = malloc(sizeof(NodoUsuario));
    if (nuevo == NULL) return NULL;

    strncpy(nuevo->nombre, nombre, MAX_NAME - 1);
    nuevo->nombre[MAX_NAME - 1] = '\0';
    nuevo->estado    = DESCONECTADO;
    nuevo->puerto    = 0;
    nuevo->ultimo_id = 0;
    memset(nuevo->ip, 0, MAX_IP);
    nuevo->siguiente = lista_usuarios;
    lista_usuarios   = nuevo;
    return nuevo;
}

/*
 * Elimina el usuario con ese nombre de la lista enlazada y libera su memoria.
 * Devuelve 0 si se elimino, -1 si no existia.
 * PRECONDICION: debe llamarse con mutex_usuarios tomado.
 */
int eliminar_usuario(const char *nombre) {
    NodoUsuario *actual   = lista_usuarios;
    NodoUsuario *anterior = NULL;

    while (actual != NULL) {
        if (strcmp(actual->nombre, nombre) == 0) {
            /* desenlazamos el nodo */
            if (anterior == NULL)
                lista_usuarios = actual->siguiente;
            else
                anterior->siguiente = actual->siguiente;
            free(actual);
            return 0;
        }
        anterior = actual;
        actual   = actual->siguiente;
    }
    return -1;
}

/* ===================== GESTION DE LA LISTA DE MENSAJES ===================== */

/*
 * Crea un nuevo nodo de mensaje e inserta al inicio de la lista.
 * Devuelve el puntero al nuevo nodo, o NULL si falla el malloc.
 * PRECONDICION: debe llamarse con mutex_mensajes tomado.
 */
NodoMensaje *insertar_mensaje(const char *destino,
                               const char *remitente,
                               unsigned int id,
                               const char *texto) {
    NodoMensaje *nuevo = malloc(sizeof(NodoMensaje));
    if (nuevo == NULL) return NULL;

    strncpy(nuevo->destino,   destino,   MAX_NAME - 1);
    strncpy(nuevo->remitente, remitente, MAX_NAME - 1);
    strncpy(nuevo->texto,     texto,     MAX_MSG_TEXT - 1);
    nuevo->destino[MAX_NAME - 1]     = '\0';
    nuevo->remitente[MAX_NAME - 1]   = '\0';
    nuevo->texto[MAX_MSG_TEXT - 1]   = '\0';
    nuevo->id        = id;
    nuevo->siguiente = lista_mensajes;
    lista_mensajes   = nuevo;
    return nuevo;
}

/*
 * Elimina el nodo de mensaje con ese id y ese destino de la lista y libera su memoria.
 * Devuelve 0 si se elimino, -1 si no se encontro.
 * PRECONDICION: debe llamarse con mutex_mensajes tomado.
 */
int eliminar_mensaje(const char *destino, unsigned int id) {
    NodoMensaje *actual   = lista_mensajes;
    NodoMensaje *anterior = NULL;

    while (actual != NULL) {
        if (actual->id == id &&
            strcmp(actual->destino, destino) == 0) {
            if (anterior == NULL)
                lista_mensajes = actual->siguiente;
            else
                anterior->siguiente = actual->siguiente;
            free(actual);
            return 0;
        }
        anterior = actual;
        actual   = actual->siguiente;
    }
    return -1;
}

/*
 * Elimina todos los mensajes pendientes cuyo destino sea 'nombre'.
 * Se llama cuando un usuario se da de baja (UNREGISTER).
 * PRECONDICION: debe llamarse con mutex_mensajes tomado.
 */
void eliminar_mensajes_de(const char *nombre) {
    NodoMensaje *actual   = lista_mensajes;
    NodoMensaje *anterior = NULL;

    while (actual != NULL) {
        if (strcmp(actual->destino, nombre) == 0) {
            NodoMensaje *aborrar = actual;
            if (anterior == NULL)
                lista_mensajes = actual->siguiente;
            else
                anterior->siguiente = actual->siguiente;
            actual = actual->siguiente;
            free(aborrar);
        } else {
            anterior = actual;
            actual   = actual->siguiente;
        }
    }
}

/* ===================== OBTENCION DE IP LOCAL ===================== */

/*
 * Obtiene la IP local de la primera interfaz que no sea loopback.
 * Si no encuentra ninguna, devuelve "127.0.0.1".
 */
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

/* ===================== ENVIO DE MENSAJES A CLIENTES ===================== */

/*
 * Conecta al hilo de escucha del cliente destinatario y le envia
 * el mensaje siguiendo el protocolo 8.6 (SEND_MESSAGE).
 * Devuelve 0 si se entrego con exito, -1 si fallo.
 */
int enviar_mensaje_a_cliente(const char *ip, int puerto,
                              const char *remitente,
                              unsigned int id,
                              const char *texto) {
    struct sockaddr_in addr;
    int   sock;
    char  buf[64];

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

    /* protocolo seccion 8.6: operacion + remitente + id + texto */
    enviar_linea(sock, "SEND_MESSAGE");
    enviar_linea(sock, remitente);
    snprintf(buf, sizeof(buf), "%u", id);
    enviar_linea(sock, buf);
    enviar_linea(sock, texto);

    close(sock);
    return 0;
}

/*
 * Notifica al remitente de un mensaje que este ha sido entregado
 * correctamente siguiendo el protocolo 8.6 (SEND_MESS_ACK).
 * Si el remitente no esta conectado, se descarta segun el enunciado.
 * PRECONDICION: debe llamarse con mutex_usuarios tomado.
 */
int enviar_ack_remitente(NodoUsuario *nodo_rem, unsigned int id) {
    struct sockaddr_in addr;
    int   sock;
    char  buf[64];

    /* si el remitente no esta conectado, descartamos el ACK */
    if (nodo_rem == NULL || nodo_rem->estado != CONECTADO) return 0;

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(nodo_rem->puerto);
    if (inet_pton(AF_INET, nodo_rem->ip, &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }
    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    /* protocolo seccion 8.6 parte 2: SEND_MESS_ACK + id */
    enviar_linea(sock, "SEND_MESS_ACK");
    snprintf(buf, sizeof(buf), "%u", id);
    enviar_linea(sock, buf);

    close(sock);
    return 0;
}

/*
 * Recorre la lista de mensajes pendientes del usuario 'nodo_dest'
 * e intenta entregarlos uno a uno.
 * Se llama justo despues de que el usuario se conecta (seccion 7.4).
 * Si un envio falla, marca al destinatario como desconectado y para.
 */
void enviar_pendientes(NodoUsuario *nodo_dest) {
    /* iteramos la lista de mensajes buscando los de este destinatario */
    while (1) {
        /* buscamos el primer mensaje pendiente para este usuario */
        pthread_mutex_lock(&mutex_mensajes);
        NodoMensaje *msg = lista_mensajes;
        char          rem[MAX_NAME], dest[MAX_NAME], texto[MAX_MSG_TEXT];
        unsigned int  id = 0;
        int           encontrado = 0;

        while (msg != NULL) {
            if (strcmp(msg->destino, nodo_dest->nombre) == 0) {
                /* copiamos los datos para no mantener el mutex durante el envio */
                strncpy(rem,   msg->remitente, MAX_NAME - 1);
                strncpy(dest,  msg->destino,   MAX_NAME - 1);
                strncpy(texto, msg->texto,      MAX_MSG_TEXT - 1);
                rem[MAX_NAME - 1]        = '\0';
                dest[MAX_NAME - 1]       = '\0';
                texto[MAX_MSG_TEXT - 1]  = '\0';
                id = msg->id;
                encontrado = 1;
                break;
            }
            msg = msg->siguiente;
        }
        pthread_mutex_unlock(&mutex_mensajes);

        if (!encontrado) break; /* no quedan mensajes pendientes */

        /* obtenemos IP y puerto del destinatario */
        pthread_mutex_lock(&mutex_usuarios);
        char ip_dest[MAX_IP];
        int  puerto_dest = nodo_dest->puerto;
        strncpy(ip_dest, nodo_dest->ip, MAX_IP - 1);
        ip_dest[MAX_IP - 1] = '\0';
        pthread_mutex_unlock(&mutex_usuarios);

        /* intentamos entregar el mensaje */
        int ok = enviar_mensaje_a_cliente(ip_dest, puerto_dest, rem, id, texto);

        if (ok == 0) {
            /* entregado con exito: log, borrado del almacen y ACK al remitente */
            printf("s> SEND MESSAGE %u FROM %s TO %s\n", id, rem, dest);
            fflush(stdout);

            pthread_mutex_lock(&mutex_mensajes);
            eliminar_mensaje(dest, id);
            pthread_mutex_unlock(&mutex_mensajes);

            pthread_mutex_lock(&mutex_usuarios);
            NodoUsuario *nodo_rem = buscar_usuario(rem);
            enviar_ack_remitente(nodo_rem, id);
            pthread_mutex_unlock(&mutex_usuarios);

        } else {
            /* fallo al entregar: marcamos al destino como desconectado y paramos */
            pthread_mutex_lock(&mutex_usuarios);
            nodo_dest->estado = DESCONECTADO;
            memset(nodo_dest->ip, 0, MAX_IP);
            nodo_dest->puerto = 0;
            pthread_mutex_unlock(&mutex_usuarios);
            break;
        }
    }
}

/* ===================== PROCESADO DE OPERACIONES ===================== */

/* --- REGISTER --- */
/*
 * Registra un nuevo usuario en el sistema.
 * Errores: 0=exito, 1=ya existe, 2=error generico.
 */
void op_register(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 2);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);

    /* verificamos que no exista ya un usuario con ese nombre */
    if (buscar_usuario(nombre) != NULL) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> REGISTER %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    /* insertamos el nuevo usuario en la lista */
    NodoUsuario *nuevo = insertar_usuario(nombre);
    pthread_mutex_unlock(&mutex_usuarios);

    if (nuevo == NULL) {
        /* fallo de malloc */
        enviar_byte(sock, 2);
        printf("s> REGISTER %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    enviar_byte(sock, 0);
    printf("s> REGISTER %s OK\n", nombre);
    fflush(stdout);
}

/* --- UNREGISTER --- */
/*
 * Da de baja a un usuario del sistema y borra sus mensajes pendientes.
 * Errores: 0=exito, 1=no existe, 2=error generico.
 */
void op_unregister(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 2);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);

    /* verificamos que el usuario exista */
    if (buscar_usuario(nombre) == NULL) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> UNREGISTER %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    /* borramos los mensajes pendientes de ese usuario antes de eliminarlo */
    pthread_mutex_lock(&mutex_mensajes);
    eliminar_mensajes_de(nombre);
    pthread_mutex_unlock(&mutex_mensajes);

    /* eliminamos el nodo del usuario de la lista */
    eliminar_usuario(nombre);
    pthread_mutex_unlock(&mutex_usuarios);

    enviar_byte(sock, 0);
    printf("s> UNREGISTER %s OK\n", nombre);
    fflush(stdout);
}

/* --- CONNECT --- */
/*
 * Conecta al usuario al servicio: guarda su IP y puerto,
 * cambia su estado a CONECTADO y entrega mensajes pendientes.
 * Errores: 0=exito, 1=no existe, 2=ya conectado, 3=error generico.
 */
void op_connect(int sock, const char *ip_cliente) {
    char nombre[MAX_NAME];
    char puerto_str[32];

    if (readLine(sock, nombre,     MAX_NAME)        <= 0) { enviar_byte(sock, 3); return; }
    if (readLine(sock, puerto_str, sizeof(puerto_str)) <= 0) { enviar_byte(sock, 3); return; }

    int puerto = atoi(puerto_str);

    pthread_mutex_lock(&mutex_usuarios);
    NodoUsuario *nodo = buscar_usuario(nombre);

    if (nodo == NULL) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> CONNECT %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    if (nodo->estado == CONECTADO) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 2);
        printf("s> CONNECT %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    /* actualizamos IP, puerto y estado del usuario */
    strncpy(nodo->ip, ip_cliente, MAX_IP - 1);
    nodo->ip[MAX_IP - 1] = '\0';
    nodo->puerto = puerto;
    nodo->estado = CONECTADO;
    pthread_mutex_unlock(&mutex_usuarios);

    enviar_byte(sock, 0);
    printf("s> CONNECT %s OK\n", nombre);
    fflush(stdout);

    /*
     * Entregamos los mensajes pendientes en el mismo hilo.
     * Re-buscamos el nodo porque el puntero puede haber cambiado
     * si otro hilo hubiera modificado la lista (aunque aqui es seguro
     * porque el nombre garantiza unicidad).
     */
    pthread_mutex_lock(&mutex_usuarios);
    nodo = buscar_usuario(nombre);
    pthread_mutex_unlock(&mutex_usuarios);
    if (nodo != NULL)
        enviar_pendientes(nodo);
}

/* --- DISCONNECT --- */
/*
 * Desconecta al usuario: limpia su IP/puerto y cambia su estado.
 * Errores: 0=exito, 1=no existe, 2=no estaba conectado, 3=error generico.
 */
void op_disconnect(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 3);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);
    NodoUsuario *nodo = buscar_usuario(nombre);

    if (nodo == NULL) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> DISCONNECT %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    if (nodo->estado != CONECTADO) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 2);
        printf("s> DISCONNECT %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    /* limpiamos IP, puerto y cambiamos estado a DESCONECTADO */
    memset(nodo->ip, 0, MAX_IP);
    nodo->puerto = 0;
    nodo->estado = DESCONECTADO;
    pthread_mutex_unlock(&mutex_usuarios);

    enviar_byte(sock, 0);
    printf("s> DISCONNECT %s OK\n", nombre);
    fflush(stdout);
}

/* --- SEND --- */
/*
 * Almacena un mensaje y, si el destinatario esta conectado, lo entrega
 * inmediatamente notificando al remitente con un ACK.
 * Errores: 0=exito (+ id), 1=usuario no existe, 2=error generico.
 */
void op_send(int sock) {
    char remitente[MAX_NAME];
    char destino[MAX_NAME];
    char texto[MAX_MSG_TEXT];

    if (readLine(sock, remitente, MAX_NAME)     <= 0) { enviar_byte(sock, 2); return; }
    if (readLine(sock, destino,   MAX_NAME)     <= 0) { enviar_byte(sock, 2); return; }
    if (readLine(sock, texto,     MAX_MSG_TEXT)  < 0) { enviar_byte(sock, 2); return; }

    pthread_mutex_lock(&mutex_usuarios);

    NodoUsuario *nodo_rem  = buscar_usuario(remitente);
    NodoUsuario *nodo_dest = buscar_usuario(destino);

    /* si alguno de los dos usuarios no existe, error 1 */
    if (nodo_rem == NULL || nodo_dest == NULL) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        return;
    }

    /* asignamos identificador al mensaje (unsigned int, empieza en 1) */
    nodo_rem->ultimo_id++;
    if (nodo_rem->ultimo_id == 0)
        nodo_rem->ultimo_id = 1; /* al desbordarse vuelve a 1 */
    unsigned int id = nodo_rem->ultimo_id;

    pthread_mutex_unlock(&mutex_usuarios);

    /* almacenamos el mensaje en la lista enlazada */
    pthread_mutex_lock(&mutex_mensajes);
    NodoMensaje *nodo_msg = insertar_mensaje(destino, remitente, id, texto);
    pthread_mutex_unlock(&mutex_mensajes);

    if (nodo_msg == NULL) {
        /* fallo de malloc al crear el mensaje */
        enviar_byte(sock, 2);
        return;
    }

    /* respondemos al remitente con exito + identificador */
    char id_str[32];
    snprintf(id_str, sizeof(id_str), "%u", id);
    enviar_byte(sock, 0);
    enviar_linea(sock, id_str);

    /* comprobamos si el destinatario esta conectado para entrega inmediata */
    pthread_mutex_lock(&mutex_usuarios);
    nodo_dest = buscar_usuario(destino); /* re-buscamos por seguridad */
    if (nodo_dest == NULL || nodo_dest->estado != CONECTADO) {
        pthread_mutex_unlock(&mutex_usuarios);
        printf("s> MESSAGE %u FROM %s TO %s STORED\n", id, remitente, destino);
        fflush(stdout);
        return;
    }

    /* copiamos IP y puerto antes de liberar el mutex */
    char ip_dest[MAX_IP];
    int  puerto_dest = nodo_dest->puerto;
    strncpy(ip_dest, nodo_dest->ip, MAX_IP - 1);
    ip_dest[MAX_IP - 1] = '\0';
    pthread_mutex_unlock(&mutex_usuarios);

    /* intentamos entregar el mensaje inmediatamente */
    int ok = enviar_mensaje_a_cliente(ip_dest, puerto_dest, remitente, id, texto);

    if (ok == 0) {
        /* entregado: log, borramos del almacen y notificamos al remitente */
        printf("s> SEND MESSAGE %u FROM %s TO %s\n", id, remitente, destino);
        fflush(stdout);

        pthread_mutex_lock(&mutex_mensajes);
        eliminar_mensaje(destino, id);
        pthread_mutex_unlock(&mutex_mensajes);

        pthread_mutex_lock(&mutex_usuarios);
        NodoUsuario *nodo_rem2 = buscar_usuario(remitente);
        enviar_ack_remitente(nodo_rem2, id);
        pthread_mutex_unlock(&mutex_usuarios);

    } else {
        /* fallo de envio: marcamos al destinatario como desconectado */
        pthread_mutex_lock(&mutex_usuarios);
        NodoUsuario *nodo_d2 = buscar_usuario(destino);
        if (nodo_d2 != NULL) {
            nodo_d2->estado = DESCONECTADO;
            memset(nodo_d2->ip, 0, MAX_IP);
            nodo_d2->puerto = 0;
        }
        pthread_mutex_unlock(&mutex_usuarios);
        printf("s> MESSAGE %u FROM %s TO %s STORED\n", id, remitente, destino);
        fflush(stdout);
    }
}

/* --- USERS --- */
/*
 * Devuelve la lista de usuarios actualmente conectados.
 * Errores: 0=exito, 1=solicitante no conectado, 2=no registrado/error.
 */
void op_users(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 2);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);

    NodoUsuario *nodo = buscar_usuario(nombre);
    if (nodo == NULL) {
        /* no registrado -> error tipo 2 segun seccion 7.7 */
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 2);
        printf("s> CONNECTEDUSERS FAIL\n");
        fflush(stdout);
        return;
    }

    if (nodo->estado != CONECTADO) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> CONNECTEDUSERS FAIL\n");
        fflush(stdout);
        return;
    }

    /*
     * Recopilamos los nombres de los usuarios conectados.
     * Usamos un array dinamico para no mantener el mutex durante el envio.
     * Reservamos espacio suficiente recorriendo primero la lista.
     */
    int n = 0;
    NodoUsuario *actual = lista_usuarios;
    while (actual != NULL) {
        if (actual->estado == CONECTADO) n++;
        actual = actual->siguiente;
    }

    /* reservamos un array de n cadenas */
    char (*conectados)[MAX_NAME] = malloc((size_t)n * sizeof(*conectados));
    if (conectados == NULL) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 2);
        printf("s> CONNECTEDUSERS FAIL\n");
        fflush(stdout);
        return;
    }

    int idx = 0;
    actual = lista_usuarios;
    while (actual != NULL) {
        if (actual->estado == CONECTADO) {
            strncpy(conectados[idx], actual->nombre, MAX_NAME - 1);
            conectados[idx][MAX_NAME - 1] = '\0';
            idx++;
        }
        actual = actual->siguiente;
    }
    pthread_mutex_unlock(&mutex_usuarios);

    /* enviamos: byte 0, numero de conectados y luego cada nombre */
    char buf[32];
    enviar_byte(sock, 0);
    snprintf(buf, sizeof(buf), "%d", n);
    enviar_linea(sock, buf);
    for (int i = 0; i < n; i++)
        enviar_linea(sock, conectados[i]);

    free(conectados);
    printf("s> CONNECTEDUSERS OK\n");
    fflush(stdout);
}

/* ===================== HILO POR CLIENTE ===================== */

/*
 * Funcion que ejecuta cada hilo.
 * Recibe el socket de la conexion aceptada como argumento.
 * Lee la operacion y la despacha al handler correspondiente.
 */
void *procesar_cliente(void *arg) {
    int *psock  = (int *)arg;
    int  sock   = *psock;
    free(psock);

    /* obtenemos la IP del cliente con getpeername */
    struct sockaddr_in peer;
    socklen_t peer_len = sizeof(peer);
    char ip_cliente[MAX_IP] = "0.0.0.0";
    if (getpeername(sock, (struct sockaddr *)&peer, &peer_len) == 0)
        inet_ntop(AF_INET, &peer.sin_addr, ip_cliente, sizeof(ip_cliente));

    /* leemos la operacion */
    char op[32];
    if (readLine(sock, op, sizeof(op)) <= 0) {
        close(sock);
        return NULL;
    }

    /* despachamos segun la operacion recibida */
    if      (strcmp(op, "REGISTER")   == 0) op_register(sock);
    else if (strcmp(op, "UNREGISTER") == 0) op_unregister(sock);
    else if (strcmp(op, "CONNECT")    == 0) op_connect(sock, ip_cliente);
    else if (strcmp(op, "DISCONNECT") == 0) op_disconnect(sock);
    else if (strcmp(op, "SEND")       == 0) op_send(sock);
    else if (strcmp(op, "USERS")      == 0) op_users(sock);
    else                                     enviar_byte(sock, 2);

    close(sock);
    return NULL;
}

/* ===================== MAIN ===================== */

int main(int argc, char *argv[]) {
    int port = -1;

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

        /* reservamos memoria para el descriptor de socket de cada cliente */
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