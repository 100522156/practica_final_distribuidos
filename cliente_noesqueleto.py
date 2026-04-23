#!/usr/bin/env python3
# client.py - Cliente de mensajería (Parte 1)
# Sistemas Distribuidos - UC3M
#
# Cliente concurrente multihilo que implementa las operaciones:
#   REGISTER, UNREGISTER, CONNECT, DISCONNECT, SEND, USERS, QUIT
#
# Uso: python3 ./client.py -s <IP> -p <PUERTO>
#
# Internamente usa dos hilos cuando está conectado:
#   - hilo principal: interfaz de usuario (lectura de comandos)
#   - hilo de escucha: recibe mensajes del servidor (SEND_MESSAGE, SEND_MESS_ACK)

import socket
import sys
import threading
import argparse

# ===================== CONSTANTES =====================
MAX_MSG = 256

# ===================== ESTADO GLOBAL =====================
# IP y puerto del servidor de mensajería
servidor_ip   = None
servidor_port = None

# nombre del usuario actualmente conectado (None si no hay ninguno)
usuario_conectado = None

# hilo y socket de escucha (se crean en CONNECT, se destruyen en DISCONNECT)
hilo_escucha     = None
sock_escucha     = None       # socket servidor de escucha del cliente
escucha_activa   = False      # flag para parar el hilo de escucha
lock_escucha     = threading.Lock()

# ===================== FUNCIONES DE RED =====================

def conectar_servidor():
    """Abre una conexion TCP al servidor de mensajeria.
       Devuelve el socket o None si falla."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((servidor_ip, servidor_port))
        return s
    except Exception:
        return None


def enviar_cadena(s, texto):
    """Envia una cadena terminada en \\0 tal como exige el protocolo."""
    datos = (texto + '\0').encode('utf-8')
    s.sendall(datos)


def recibir_byte(s):
    """Lee exactamente 1 byte de resultado del servidor.
       Devuelve el valor entero o -1 si falla."""
    try:
        dato = s.recv(1)
        if not dato:
            return -1
        return dato[0]
    except Exception:
        return -1


def recibir_cadena(s):
    """Lee caracteres hasta encontrar '\\0' o '\\n'.
       Devuelve la cadena leida (sin el terminador) o None si hay error."""
    resultado = b''
    try:
        while True:
            c = s.recv(1)
            if not c:
                return None
            if c == b'\0' or c == b'\n':
                break
            resultado += c
        return resultado.decode('utf-8')
    except Exception:
        return None

# ===================== HILO DE ESCUCHA =====================

def hilo_listener(puerto_escucha):
    """
    Hilo que escucha conexiones entrantes del servidor.
    Atiende dos tipos de mensajes (protocolo 8.6):
      - SEND_MESSAGE  : el servidor entrega un mensaje de otro usuario
      - SEND_MESS_ACK : confirmacion de que nuestro mensaje fue entregado
    """
    global sock_escucha, escucha_activa

    # creamos el socket de escucha del cliente
    try:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('', puerto_escucha))
        srv.listen(10)
        srv.settimeout(1.0)  # timeout para poder comprobar el flag de parada
    except Exception:
        return

    with lock_escucha:
        sock_escucha = srv

    while escucha_activa:
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            continue
        except Exception:
            break

        # atendemos cada conexion entrante
        try:
            op = recibir_cadena(conn)
            if op == "SEND_MESSAGE":
                # protocolo 8.6: remitente, id, texto
                remitente = recibir_cadena(conn)
                id_msg    = recibir_cadena(conn)
                mensaje   = recibir_cadena(conn)
                # mostramos segun el formato del enunciado seccion 6.7
                print(f"\ns> MESSAGE {id_msg} FROM {remitente}")
                print(f"   {mensaje}")
                print("   END")
                print("c> ", end='', flush=True)

            elif op == "SEND_MESS_ACK":
                # protocolo 8.6 parte 2: id del mensaje confirmado
                id_msg = recibir_cadena(conn)
                print(f"\nc> SEND MESSAGE {id_msg} OK")
                print("c> ", end='', flush=True)

        except Exception:
            pass
        finally:
            conn.close()

    srv.close()


def buscar_puerto_libre():
    """Busca un puerto TCP libre abriendo un socket temporal."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    puerto = s.getsockname()[1]
    s.close()
    return puerto

# ===================== OPERACIONES DEL CLIENTE =====================

def cmd_register(nombre):
    """Registra al usuario en el servidor (seccion 6.2 y protocolo 8.1)."""
    s = conectar_servidor()
    if s is None:
        print("c> REGISTER FAIL")
        return

    try:
        enviar_cadena(s, "REGISTER")
        enviar_cadena(s, nombre)
        codigo = recibir_byte(s)
    except Exception:
        codigo = -1
    finally:
        s.close()

    if   codigo == 0:  print("c> REGISTER OK")
    elif codigo == 1:  print("c> USERNAME IN USE")
    else:              print("c> REGISTER FAIL")


def cmd_unregister(nombre):
    """Da de baja al usuario en el servidor (seccion 6.3 y protocolo 8.2)."""
    s = conectar_servidor()
    if s is None:
        print("c> UNREGISTER FAIL")
        return

    try:
        enviar_cadena(s, "UNREGISTER")
        enviar_cadena(s, nombre)
        codigo = recibir_byte(s)
    except Exception:
        codigo = -1
    finally:
        s.close()

    if   codigo == 0:  print("c> UNREGISTER OK")
    elif codigo == 1:  print("c> USER DOES NOT EXIST")
    else:              print("c> UNREGISTER FAIL")


def cmd_connect(nombre):
    """Conecta al usuario al servicio de mensajeria (seccion 6.4 y protocolo 8.3).
       1. Busca un puerto libre
       2. Crea el hilo de escucha
       3. Envia la peticion de conexion al servidor
    """
    global usuario_conectado, hilo_escucha, escucha_activa

    if usuario_conectado is not None:
        print("c> USER ALREADY CONNECTED")
        return

    # 1. buscamos puerto libre (paso 1 del proceso de conexion, figura 2)
    puerto = buscar_puerto_libre()

    # 2. creamos el hilo de escucha antes de enviar la peticion (paso 2)
    escucha_activa = True
    hilo_escucha = threading.Thread(target=hilo_listener,
                                     args=(puerto,),
                                     daemon=True)
    hilo_escucha.start()

    # 3. enviamos la solicitud de conexion al servidor (paso 3)
    s = conectar_servidor()
    if s is None:
        escucha_activa = False
        print("c> CONNECT FAIL")
        return

    try:
        enviar_cadena(s, "CONNECT")
        enviar_cadena(s, nombre)
        enviar_cadena(s, str(puerto))    # puerto como cadena (protocolo 8.3)
        codigo = recibir_byte(s)
    except Exception:
        codigo = -1
    finally:
        s.close()

    if codigo == 0:
        usuario_conectado = nombre
        print("c> CONNECT OK")
    elif codigo == 1:
        escucha_activa = False
        print("c> CONNECT FAIL, USER DOES NOT EXIST")
    elif codigo == 2:
        escucha_activa = False
        print("c> USER ALREADY CONNECTED")
    else:
        escucha_activa = False
        print("c> CONNECT FAIL")


def cmd_disconnect(nombre):
    """Desconecta al usuario del servicio (seccion 6.5 y protocolo 8.4).
       Siempre para el hilo de escucha, aunque haya error (segun enunciado).
    """
    global usuario_conectado, hilo_escucha, escucha_activa

    s = conectar_servidor()
    if s is None:
        # aunque falle la comunicacion, paramos el hilo (segun el enunciado)
        escucha_activa = False
        usuario_conectado = None
        print("c> DISCONNECT FAIL")
        return

    try:
        enviar_cadena(s, "DISCONNECT")
        enviar_cadena(s, nombre)
        codigo = recibir_byte(s)
    except Exception:
        codigo = -1
    finally:
        s.close()

    # en cualquier caso paramos el hilo de escucha (seccion 6.5)
    escucha_activa = False
    usuario_conectado = None

    if   codigo == 0:  print("c> DISCONNECT OK")
    elif codigo == 1:  print("c> DISCONNECT FAIL, USER DOES NOT EXIST")
    elif codigo == 2:  print("c> DISCONNECT FAIL, USER NOT CONNECTED")
    else:              print("c> DISCONNECT FAIL")


def cmd_send(nombre_remitente, nombre_destino, mensaje):
    """Envia un mensaje a otro usuario (seccion 6.6 y protocolo 8.5)."""
    s = conectar_servidor()
    if s is None:
        print("c> SEND FAIL")
        return

    try:
        enviar_cadena(s, "SEND")
        enviar_cadena(s, nombre_remitente)
        enviar_cadena(s, nombre_destino)
        enviar_cadena(s, mensaje)
        codigo = recibir_byte(s)

        if codigo == 0:
            # en caso de exito el servidor devuelve ademas el identificador
            id_msg = recibir_cadena(s)
            print(f"c> SEND OK - MESSAGE {id_msg}")
        elif codigo == 1:
            print("c> SEND FAIL, USER DOES NOT EXIST")
        else:
            print("c> SEND FAIL")
    except Exception:
        print("c> SEND FAIL")
    finally:
        s.close()


def cmd_users(nombre):
    """Solicita la lista de usuarios conectados (seccion 6.8 y protocolo 8.7)."""
    s = conectar_servidor()
    if s is None:
        print("c> CONNECTED USERS FAIL")
        return

    try:
        enviar_cadena(s, "USERS")
        enviar_cadena(s, nombre)
        codigo = recibir_byte(s)

        if codigo == 0:
            # recibimos el numero de conectados y luego cada nombre
            num_str = recibir_cadena(s)
            n = int(num_str) if num_str else 0
            print(f"c> CONNECTED USERS ({n} users connected) OK")
            for _ in range(n):
                user = recibir_cadena(s)
                if user is not None:
                    print(f"   {user}")
        elif codigo == 1:
            print("c> CONNECTED USERS FAIL, USER IS NOT CONNECTED")
        else:
            print("c> CONNECTED USERS FAIL")
    except Exception:
        print("c> CONNECTED USERS FAIL")
    finally:
        s.close()

# ===================== BUCLE PRINCIPAL =====================

def main():
    global servidor_ip, servidor_port

    # parseamos argumentos: python3 client.py -s <IP> -p <PUERTO>
    parser = argparse.ArgumentParser(description='Cliente de mensajeria SD')
    parser.add_argument('-s', required=True, help='IP del servidor')
    parser.add_argument('-p', required=True, type=int, help='Puerto del servidor')
    args = parser.parse_args()

    servidor_ip   = args.s
    servidor_port = args.p

    # bucle de interaccion con el usuario
    while True:
        try:
            linea = input("c> ").strip()
        except (EOFError, KeyboardInterrupt):
            # CTRL+D o CTRL+C cierran el cliente igual que QUIT
            break

        if not linea:
            continue

        partes = linea.split(' ', 2)   # separamos en maximo 3 partes
        cmd    = partes[0].upper()

        if cmd == "QUIT":
            # si hay un usuario conectado lo desconectamos antes de salir
            if usuario_conectado is not None:
                cmd_disconnect(usuario_conectado)
            break

        elif cmd == "REGISTER":
            if len(partes) < 2:
                print("c> Uso: REGISTER <userName>")
            else:
                cmd_register(partes[1])

        elif cmd == "UNREGISTER":
            if len(partes) < 2:
                print("c> Uso: UNREGISTER <userName>")
            else:
                cmd_unregister(partes[1])

        elif cmd == "CONNECT":
            if len(partes) < 2:
                print("c> Uso: CONNECT <userName>")
            else:
                cmd_connect(partes[1])

        elif cmd == "DISCONNECT":
            if usuario_conectado is None:
                # si no hay usuario conectado usamos el nombre del argumento
                if len(partes) < 2:
                    print("c> Uso: DISCONNECT <userName>")
                else:
                    cmd_disconnect(partes[1])
            else:
                cmd_disconnect(usuario_conectado)

        elif cmd == "SEND":
            if len(partes) < 3:
                print("c> Uso: SEND <userName> <message>")
            elif usuario_conectado is None:
                print("c> SEND FAIL")
            else:
                cmd_send(usuario_conectado, partes[1], partes[2])

        elif cmd == "USERS":
            if usuario_conectado is None:
                print("c> CONNECTED USERS FAIL, USER IS NOT CONNECTED")
            else:
                cmd_users(usuario_conectado)

        elif cmd == "SENDATTACH":
            # no implementado en la parte 1
            print("c> SENDATTACH not implemented in Part 1")

        else:
            print(f"c> Comando desconocido: {cmd}")

    # paramos el hilo de escucha si estaba activo al salir
    global escucha_activa
    escucha_activa = False


if __name__ == '__main__':
    main()