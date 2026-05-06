/*
 * server.c - Servidor de mensajeria (Parte 1 + Parte 2: ficheros adjuntos)
 * Sistemas Distribuidos - UC3M
 *
 * Servidor concurrente multihilo que gestiona:
 *  - Registro/baja de usuarios
 *  - Conexion/desconexion
 *  - Envio y almacenamiento de mensajes pendientes (con y sin fichero adjunto)
 *  - Listado de usuarios conectados (devuelve IP y puerto por usuario)
 *  - Transferencia de ficheros entre clientes (via GET_FILE al hilo de escucha)
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

//constantes
#define MAX_NAME     256
#define MAX_MSG_TEXT 256
#define MAX_FILENAME 256
#define MAX_IP       64

//aqui desarollare las estructuras

/* Estado posible de un usuario */
typedef enum { DESCONECTADO, CONECTADO } EstadoUsuario;

//nodo de la lista enlazada de usuarios, cada nodo contiene la informacion de un usuario registrado y un puntero al siguiente usuario
typedef struct NodoUsuario {
    char nombre[MAX_NAME];
    EstadoUsuario estado;
    char ip[MAX_IP];
    int puerto;
    unsigned int ultimo_id; /* ultimo identificador de mensaje asignado */
    struct NodoUsuario *siguiente;
} NodoUsuario;

/*
 * Nodo de la lista enlazada de mensajes pendientes.
 * Cada nodo almacena un mensaje pendiente de entrega.
 * El campo filename estara vacio si el mensaje no tiene adjunto.
 * El campo tiene_adjunto indica si es un SENDATTACH o un SEND normal.
 */
// nodo de la lista enlazada de mensajes pendientes, cada nodo almacena un mensaje pendiente de entrega
typedef struct NodoMensaje {
    char destino[MAX_NAME];   /* usuario destinatario */
    char remitente[MAX_NAME];  /* usuario que lo envio */
    unsigned int id;  /* identificador del mensaje */
    char texto[MAX_MSG_TEXT];  /* contenido del mensaje */
    char filename[MAX_FILENAME]; /* nombre del fichero adjunto (puede ser "") */
    int tiene_adjunto;  /* 1 si es SENDATTACH, 0 si es SEND */
    struct NodoMensaje *siguiente;
} NodoMensaje;

/* Cabeceras de las listas enlazadas */
NodoUsuario *lista_usuarios = NULL;
NodoMensaje *lista_mensajes = NULL;

/* mutex para proteger acceso concurrente a las listas */
pthread_mutex_t mutex_usuarios = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_mensajes = PTHREAD_MUTEX_INITIALIZER;

//las funciones de red dadas en clsae

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
//aqui gestionamos la lista de usuarios

//buscamos a un usuario por nombre en la lista enlazada y devuelve un puntero hacia el o null
NodoUsuario *buscar_usuario(const char *nombre) {
    NodoUsuario *actual = lista_usuarios;
    while (actual != NULL) {
        if (strcmp(actual->nombre, nombre) == 0)
            return actual;
        actual = actual->siguiente;
    }
    return NULL;
}

//creo un nuevo nodo de usuario y lo inserto al princpio de la lista
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

//elimino al usaurio y libero su memoria 
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


//gestion de la lista de mensajes

//creo un nuevo nodo de mensaje e inserto al princpio de la lista
NodoMensaje *insertar_mensaje(const char *destino, const char *remitente,unsigned int id, const char *texto,const char *filename, int tiene_adjunto) {

    NodoMensaje *nuevo = malloc(sizeof(NodoMensaje));
    if (nuevo == NULL) return NULL;

    strncpy(nuevo->destino,destino,MAX_NAME - 1);
    strncpy(nuevo->remitente,remitente, MAX_NAME - 1);
    strncpy(nuevo->texto,texto, MAX_MSG_TEXT - 1);
    strncpy(nuevo->filename,filename, MAX_FILENAME - 1);
    nuevo->destino[MAX_NAME - 1] = '\0';//pongo un \0 por si es un elemento mayor
    nuevo->remitente[MAX_NAME - 1] = '\0';//pongo un \0 por si es un elemento mayor
    nuevo->texto[MAX_MSG_TEXT - 1] = '\0';//pongo un \0 por si es un elemento mayor
    nuevo->filename[MAX_FILENAME - 1] = '\0';//pongo un \0 por si es un elemento mayor
    nuevo->id = id;
    nuevo->tiene_adjunto  = tiene_adjunto;
    nuevo->siguiente  = lista_mensajes;//inserto el nodo en la lista 
    lista_mensajes  = nuevo;//hago que la lista apunte a este nodo
    return nuevo;
}

//elimino el nodo de mensaje con ese id y destino
int eliminar_mensaje(const char *destino, unsigned int id) {
    NodoMensaje *actual   = lista_mensajes;
    NodoMensaje *anterior = NULL;

    while (actual != NULL) {
        if (actual->id == id && strcmp(actual->destino, destino) == 0) {
            if (anterior == NULL)
                lista_mensajes = actual->siguiente;//si es el primero solo se hace que se apunte la lista al siguiente
            else
                anterior->siguiente = actual->siguiente;//si no es el primero se salta ese nodo que se quiere borrar
            free(actual);
            return 0;
        }
        //y esto para cuando no sea el nodo q busco paso al siguiente
        anterior = actual;
        actual   = actual->siguiente;
    }
    return -1;
}

//elimino todos los mensajes pendientes cuyo destino sea nombre
void eliminar_mensajes_de(const char *nombre) {
    NodoMensaje *actual   = lista_mensajes;
    NodoMensaje *anterior = NULL;

    while (actual != NULL) {
        if (strcmp(actual->destino, nombre) == 0) {//busco el mensaje con el id que quiero borrar a la persona q se lo he enviado
            NodoMensaje *aborrar = actual;//lo asignas para luego borrar esta direccion de memoria porque es del nodo que no quieres
            if (anterior == NULL)
                lista_mensajes = actual->siguiente;
            else
                anterior->siguiente = actual->siguiente;
            actual = actual->siguiente;
            free(aborrar);//aqui borras el nodo del destinatario
        } else {
            anterior = actual;
            actual   = actual->siguiente;
        }
    }
}

//obtengo la IP local de la primera interfaz que no sea un loopback , si no encuentro ninguna ip ps se devuelve 127.0.0.1
void obtener_ip_local(char *ip_buf, size_t len) {
    struct ifaddrs *ifaddr, *ifa;//inicializo mi futura lista de interfaces de red y el puntero para moverme en ella
    if (getifaddrs(&ifaddr) == -1) {//aqui creo la lista
        strncpy(ip_buf, "127.0.0.1", len);//si me da error al crear copio el ip host y ya
        return;
    }
    strncpy(ip_buf, "127.0.0.1", len);
    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL) continue;
        if (ifa->ifa_addr->sa_family != AF_INET) continue;
        struct sockaddr_in *sa = (struct sockaddr_in *)ifa->ifa_addr;//aqui le indico que es una estructura de ipv4 para que la trate como tal
        char tmp[INET_ADDRSTRLEN];//reservo espacio para guardar la ip como string podria poner 16 tb
        inet_ntop(AF_INET, &sa->sin_addr, tmp, sizeof(tmp));
        if (strcmp(tmp, "127.0.0.1") != 0) {//aqui si da no es igual que el host lo  copio en el ip_buf
            strncpy(ip_buf, tmp, len);
            break;
        }
    }
    freeifaddrs(ifaddr);//libero la lista de ips
}

//envio el mensaje hacia el cliente que me pide cuando hago el send , usando un socket
int enviar_mensaje_a_cliente(const char *ip, int puerto,const char *remitente,unsigned int id, const char *texto) {
    struct sockaddr_in addr;
    int  sock;
    char buf[64];

    sock = socket(AF_INET, SOCK_STREAM, 0);//creo el socket
    if (sock < 0) return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(puerto);//paso el puerto a modo red para hacer el connect
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {//pasoa  formato red la ip para hacer el connect
        close(sock);
        return -1;
    }
    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {//conecto el servidor con el usuario remitente
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

//conecto al hilo de escucha del cliente destinatario y le envio un mensaje con fichero adjunto segun el protocolo 
int enviar_mensaje_attach_a_cliente(const char *ip, int puerto,const char *remitente,unsigned int id, const char *texto,const char *filename) {
    struct sockaddr_in addr;
    int  sock;
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

    /* protocolo parte 2 seccion 2.3: operacion + remitente + id + texto + filename */
    enviar_linea(sock, "SEND_MESSAGE_ATTACH");
    enviar_linea(sock, remitente);
    snprintf(buf, sizeof(buf), "%u", id);
    enviar_linea(sock, buf);
    enviar_linea(sock, texto);
    enviar_linea(sock, filename);

    close(sock);
    return 0;
}

// notifica al remitente de un mensaje que este ha isdo entregado correctamente 
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
    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {//conectnto con el remitente para decirle si ha funcinado correctamente
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

//notifico al remitente de un mensaje con adjunto entregado
int enviar_ack_attach_remitente(NodoUsuario *nodo_rem, unsigned int id,const char *filename) {
    struct sockaddr_in addr;
    int   sock;
    char  buf[64];

    /* si el remitente no esta conectado, descartamos el ACK (seccion 2.3) */
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

    /* protocolo parte 2: SEND_MESS_ATTACH_ACK + id + filename */
    enviar_linea(sock, "SEND_MESS_ATTACH_ACK");
    snprintf(buf, sizeof(buf), "%u", id);
    enviar_linea(sock, buf);
    enviar_linea(sock, filename);

    close(sock);
    return 0;
}


 //recorro la lista de mensajes pendientes del usuario destiantario y se llama justo despues de que el usuario se conecte
void enviar_pendientes(NodoUsuario *nodo_dest) {
    while (1) {
        /* buscamos el primer mensaje pendiente para este usuario */
        pthread_mutex_lock(&mutex_mensajes);
        NodoMensaje *msg = lista_mensajes;
        char         rem[MAX_NAME], dest[MAX_NAME];
        char         texto[MAX_MSG_TEXT], fname[MAX_FILENAME];
        unsigned int id = 0;
        int          encontrado = 0;
        int          tiene_adj  = 0;

        while (msg != NULL) {
            if (strcmp(msg->destino, nodo_dest->nombre) == 0) {
                strncpy(rem,   msg->remitente, MAX_NAME - 1);
                strncpy(dest,  msg->destino,   MAX_NAME - 1);
                strncpy(texto, msg->texto,      MAX_MSG_TEXT - 1);
                strncpy(fname, msg->filename,   MAX_FILENAME - 1);
                rem[MAX_NAME - 1]         = '\0';
                dest[MAX_NAME - 1]        = '\0';
                texto[MAX_MSG_TEXT - 1]   = '\0';
                fname[MAX_FILENAME - 1]   = '\0';
                id         = msg->id;
                tiene_adj  = msg->tiene_adjunto;
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

        /* intentamos entregar el mensaje segun si tiene adjunto o no */
        int ok;
        if (tiene_adj)
            ok = enviar_mensaje_attach_a_cliente(ip_dest, puerto_dest,rem, id, texto, fname);
        else
            ok = enviar_mensaje_a_cliente(ip_dest, puerto_dest, rem, id, texto);

        if (ok == 0) {
            /* entregado con exito: log, borrado del almacen y ACK al remitente */
            printf("s> SEND MESSAGE %u FROM %s TO %s\n", id, rem, dest);
            fflush(stdout);

            pthread_mutex_lock(&mutex_mensajes);
            eliminar_mensaje(dest, id);
            pthread_mutex_unlock(&mutex_mensajes);

            pthread_mutex_lock(&mutex_usuarios);
            NodoUsuario *nodo_rem = buscar_usuario(rem);
            if (tiene_adj)
                enviar_ack_attach_remitente(nodo_rem, id, fname);
            else
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

//operaciones

//registra un nuevo usuario en el sistema , siendo estos los errores 0=exito, 1=ya existe, 2=error generico
void op_register(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {//se guarda primero el nombre
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

//unregister

//da de baja a un usuario del sistema y borra sus mensajes pendientes
void op_unregister(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 2);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);

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

    eliminar_usuario(nombre);
    pthread_mutex_unlock(&mutex_usuarios);

    enviar_byte(sock, 0);
    printf("s> UNREGISTER %s OK\n", nombre);
    fflush(stdout);
}

//connect 
//conecta al usuario al servicio y guarda su ip y su puerto los errores:0=exito, 1=no existe, 2=ya conectado, 3=error cualquiera.
void op_connect(int sock, const char *ip_cliente) {
    char nombre[MAX_NAME];
    char puerto_str[32];

    if (readLine(sock, nombre,     MAX_NAME) <= 0) { enviar_byte(sock, 3); return; }
    if (readLine(sock, puerto_str, sizeof(puerto_str)) <= 0) { enviar_byte(sock, 3); return; }

    int puerto = atoi(puerto_str);//paso a numero el puerto para guardarlo

    pthread_mutex_lock(&mutex_usuarios);
    NodoUsuario *nodo = buscar_usuario(nombre);

    if (nodo == NULL) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> CONNECT %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    if (nodo->estado == CONECTADO) {//compruebo que ya esta conectado
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

    /* entregamos los mensajes pendientes en el mismo hilo */
    pthread_mutex_lock(&mutex_usuarios);
    nodo = buscar_usuario(nombre);
    pthread_mutex_unlock(&mutex_usuarios);
    if (nodo != NULL)
        enviar_pendientes(nodo);
}

//disconnect
 //desconecnta al usuario con todo lo que eso conlleva siendo estos los errores 0=exito, 1=no existe, 2=no estaba conectado, 3=error generico.
void op_disconnect(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 3);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);
    NodoUsuario *nodo = buscar_usuario(nombre);

    if (nodo == NULL) {//compruebo que existe
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        printf("s> DISCONNECT %s FAIL\n", nombre);
        fflush(stdout);
        return;
    }

    if (nodo->estado != CONECTADO) {//y tambien que este conncetado
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

//send 
//almaceno un mensaje y si el destinatario esta conectado lo entrego
void op_send(int sock) {
    char remitente[MAX_NAME];
    char destino[MAX_NAME];
    char texto[MAX_MSG_TEXT];

    if (readLine(sock, remitente, MAX_NAME) <= 0) { enviar_byte(sock, 2); return; }
    if (readLine(sock, destino,   MAX_NAME) <= 0) { enviar_byte(sock, 2); return; }
    if (readLine(sock, texto,     MAX_MSG_TEXT) < 0) { enviar_byte(sock, 2); return; }

    pthread_mutex_lock(&mutex_usuarios);

    NodoUsuario *nodo_rem  = buscar_usuario(remitente);
    NodoUsuario *nodo_dest = buscar_usuario(destino);

    if (nodo_rem == NULL || nodo_dest == NULL) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        return;
    }

    /* asignamos identificador al mensaje */
    nodo_rem->ultimo_id++;
    if (nodo_rem->ultimo_id == 0)
        nodo_rem->ultimo_id = 1;
    unsigned int id = nodo_rem->ultimo_id;

    pthread_mutex_unlock(&mutex_usuarios);

    /* almacenamos el mensaje (sin adjunto) */
    pthread_mutex_lock(&mutex_mensajes);
    NodoMensaje *nodo_msg = insertar_mensaje(destino, remitente, id, texto, "", 0);
    pthread_mutex_unlock(&mutex_mensajes);

    if (nodo_msg == NULL) {
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
    nodo_dest = buscar_usuario(destino);
    if (nodo_dest == NULL || nodo_dest->estado != CONECTADO) {
        pthread_mutex_unlock(&mutex_usuarios);
        printf("s> MESSAGE %u FROM %s TO %s STORED\n", id, remitente, destino);
        fflush(stdout);
        return;
    }

    char ip_dest[MAX_IP];
    int  puerto_dest = nodo_dest->puerto;
    strncpy(ip_dest, nodo_dest->ip, MAX_IP - 1);
    ip_dest[MAX_IP - 1] = '\0';
    pthread_mutex_unlock(&mutex_usuarios);

    /* intentamos entregar el mensaje inmediatamente */
    int ok = enviar_mensaje_a_cliente(ip_dest, puerto_dest, remitente, id, texto);

    if (ok == 0) {
        //borramos el mensaje cuando ha sido enviado y ya lo puedo quitar de la lista de mensajes
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

//sendattach almacena un mensaje con un fichero adjunto(su ruta) , si el distinatario esta conectado lo entrega como la seccion 2.2
void op_sendattach(int sock) {
    char remitente[MAX_NAME];
    char destino[MAX_NAME];
    char texto[MAX_MSG_TEXT];
    char filename[MAX_FILENAME];

    if (readLine(sock, remitente, MAX_NAME) <= 0) { enviar_byte(sock, 2); return; }
    if (readLine(sock, destino, MAX_NAME) <= 0) { enviar_byte(sock, 2); return; }
    if (readLine(sock, texto, MAX_MSG_TEXT) < 0) { enviar_byte(sock, 2); return; }
    if (readLine(sock, filename, MAX_FILENAME) <= 0) { enviar_byte(sock, 2); return; }

    pthread_mutex_lock(&mutex_usuarios);

    NodoUsuario *nodo_rem  = buscar_usuario(remitente);
    NodoUsuario *nodo_dest = buscar_usuario(destino);

    if (nodo_rem == NULL || nodo_dest == NULL) {
        pthread_mutex_unlock(&mutex_usuarios);
        enviar_byte(sock, 1);
        return;
    }

    /* asignamos identificador al mensaje */
    nodo_rem->ultimo_id++;
    if (nodo_rem->ultimo_id == 0)
        nodo_rem->ultimo_id = 1;
    unsigned int id = nodo_rem->ultimo_id;

    pthread_mutex_unlock(&mutex_usuarios);

    /* almacenamos el mensaje con adjunto (tiene_adjunto = 1) */
    pthread_mutex_lock(&mutex_mensajes);
    NodoMensaje *nodo_msg = insertar_mensaje(destino, remitente, id, texto, filename, 1);
    pthread_mutex_unlock(&mutex_mensajes);

    if (nodo_msg == NULL) {
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
    nodo_dest = buscar_usuario(destino);
    if (nodo_dest == NULL || nodo_dest->estado != CONECTADO) {
        pthread_mutex_unlock(&mutex_usuarios);
        printf("s> MESSAGE %u FROM %s TO %s STORED\n", id, remitente, destino);
        fflush(stdout);
        return;
    }

    char ip_dest[MAX_IP];
    int  puerto_dest = nodo_dest->puerto;
    strncpy(ip_dest, nodo_dest->ip, MAX_IP - 1);
    ip_dest[MAX_IP - 1] = '\0';
    pthread_mutex_unlock(&mutex_usuarios);

    /* intentamos entregar el mensaje con adjunto inmediatamente */
    int ok = enviar_mensaje_attach_a_cliente(ip_dest, puerto_dest,remitente, id, texto, filename);

    if (ok == 0) {
        printf("s> SEND MESSAGE %u FROM %s TO %s\n", id, remitente, destino);
        fflush(stdout);

        pthread_mutex_lock(&mutex_mensajes);
        eliminar_mensaje(destino, id);
        pthread_mutex_unlock(&mutex_mensajes);

        pthread_mutex_lock(&mutex_usuarios);
        NodoUsuario *nodo_rem2 = buscar_usuario(remitente);
        enviar_ack_attach_remitente(nodo_rem2, id, filename);
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

//users
//devuelve la lista de usuarios actualmente conectados con estos errores: 0=exito, 1=solicitante no conectado, 2=no registrado/error.
void op_users(int sock) {
    char nombre[MAX_NAME];
    if (readLine(sock, nombre, MAX_NAME) <= 0) {
        enviar_byte(sock, 2);
        return;
    }

    pthread_mutex_lock(&mutex_usuarios);

    NodoUsuario *nodo = buscar_usuario(nombre);
    if (nodo == NULL) {
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

    /* contamos cuantos usuarios estan conectados */
    int n = 0;
    NodoUsuario *actual = lista_usuarios;
    while (actual != NULL) {
        if (actual->estado == CONECTADO) n++;
        actual = actual->siguiente;
    }

    //reservo un array de cadenas para poder almacenar entradas con el formate que pide en la seccion 2.4
    int  entry_size = MAX_NAME + MAX_IP + 32;
    char (*conectados)[MAX_NAME + MAX_IP + 32] = malloc((size_t)n * entry_size);
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
            /* formato: "nombre :: IP :: puerto" */
            snprintf(conectados[idx], entry_size, "%s :: %s :: %d",
                     actual->nombre, actual->ip, actual->puerto);
            idx++;
        }
        actual = actual->siguiente;
    }
    pthread_mutex_unlock(&mutex_usuarios);

    /* enviamos: byte 0, numero de conectados y luego cada entrada */
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

//funcion del hilo por cliente

//la funcion que ejecuta cada hilo recibe el socket de la conexion aceptada como argumento lee la operacion y luego decide
void *procesar_cliente(void *arg) {
    int *psock = (int *)arg;
    int  sock  = *psock;
    free(psock);

    /* obtenemos la IP del cliente con getpeername */
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    char ip_cliente[MAX_IP] = "0.0.0.0";
    if (getpeername(sock, (struct sockaddr *)&client_addr, &client_addr_len) == 0)
        inet_ntop(AF_INET, &client_addr.sin_addr, ip_cliente, sizeof(ip_cliente));

    /* leemos la operacion */
    char op[32];
    if (readLine(sock, op, sizeof(op)) <= 0) {
        close(sock);
        return NULL;
    }

    /* despachamos segun la operacion recibida */
    if      (strcmp(op, "REGISTER")    == 0) op_register(sock);
    else if (strcmp(op, "UNREGISTER")  == 0) op_unregister(sock);
    else if (strcmp(op, "CONNECT")     == 0) op_connect(sock, ip_cliente);
    else if (strcmp(op, "DISCONNECT")  == 0) op_disconnect(sock);
    else if (strcmp(op, "SEND")        == 0) op_send(sock);
    else if (strcmp(op, "SENDATTACH")  == 0) op_sendattach(sock);
    else if (strcmp(op, "USERS")       == 0) op_users(sock);
    else                                      enviar_byte(sock, 2);

    close(sock);
    return NULL;
}

//main

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