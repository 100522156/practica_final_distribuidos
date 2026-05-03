"""
client.py - Cliente de mensajeria (Parte 1 + Parte 2: ficheros adjuntos)
Sistemas Distribuidos - UC3M

Cliente concurrente multihilo que gestiona:
 - Registro/baja de usuarios
 - Conexion/desconexion al servicio de mensajeria
 - Envio de mensajes a otros usuarios (con y sin fichero adjunto)
 - Recepcion de mensajes del servidor (hilo de escucha)
 - Listado de usuarios conectados (con IP y puerto)
 - Transferencia de ficheros entre clientes (GETFILE)

Protocolo: sockets TCP, cadenas terminadas en '\n' (leidas con readline),
           un byte de respuesta del servidor.
Uso: python3 client.py -s <IP_servidor> -p <puerto_servidor>
"""

import socket
import threading
import argparse
import sys
import os

# ===================== CONSTANTES =====================
MAX_NAME     = 256   # longitud maxima del nombre de usuario
MAX_MSG_TEXT = 256   # longitud maxima del texto de un mensaje
MAX_FILENAME = 256   # longitud maxima del nombre de fichero

# ===================== CLASE CLIENTE =====================

class client:
    """
    Clase estatica que implementa el protocolo cliente del servicio de mensajeria.
    Mantiene el estado de la conexion actual (usuario conectado, hilo de escucha, etc.)
    como atributos de clase.
    """

    # ==================== TIPOS ====================

    class RC:
        """Codigos de retorno de las operaciones del protocolo."""
        OK         = 0
        ERROR      = 1
        USER_ERROR = 2

    # ================== ATRIBUTOS ==================

    _server  = None   # IP del servidor de mensajeria
    _port    = -1     # Puerto del servidor de mensajeria

    # Estado de la conexion actual del cliente
    _listen_sock    = None    # socket servidor del hilo de escucha
    _listen_port    = -1      # puerto en el que escucha el hilo de escucha
    _listen_thread  = None    # hilo de escucha de mensajes entrantes
    _connected_user = None    # nombre del usuario actualmente conectado
    _stop_event     = None    # evento para detener el hilo de escucha

    # Estructura de datos de usuarios conectados: dict nombre -> (ip, puerto)
    # Se actualiza cada vez que se ejecuta USERS (seccion 2.4 parte 2)
    _users_info = {}          # {nombre: (ip, puerto)}
    _users_lock = threading.Lock()  # mutex para proteger _users_info

    # ================= UTILIDADES DE RED =================

    @staticmethod
    def _send_line(sock, texto):
        """
        Envia una cadena de texto por el socket seguida de un salto de linea '\n'.
        El servidor lee lineas hasta '\n' o '\0'.

        :param sock:  socket TCP ya conectado
        :param texto: cadena de texto a enviar (sin terminador)
        """
        # codificamos la cadena con terminador de linea y la enviamos completa
        mensaje = (texto + "\n").encode()
        sock.sendall(mensaje)

    @staticmethod
    def _recv_byte(sock):
        """
        Recibe exactamente 1 byte del socket y lo devuelve como entero.
        Devuelve -1 si la conexion se cierra antes de recibir el byte.

        :param sock: socket TCP ya conectado
        :return:     valor del byte recibido (0-255) o -1 en error
        """
        # leemos el byte de respuesta del servidor (codigo de operacion)
        data = sock.recv(1)
        if not data:
            return -1
        return data[0]

    @staticmethod
    def _recv_line(sock):
        """
        Lee caracteres del socket hasta encontrar '\n' o '\0'.
        Devuelve la cadena sin el terminador, o None si se cierra la conexion.

        :param sock: socket TCP ya conectado
        :return:     cadena leida o None en error
        """
        # acumulamos bytes hasta encontrar el terminador de linea
        buf = b""
        while True:
            ch = sock.recv(1)
            if not ch:
                return None
            if ch == b"\n" or ch == b"\0":
                break
            buf += ch
        return buf.decode(errors="replace")

    @staticmethod
    def _connect_to_server():
        """
        Crea y devuelve un socket TCP conectado al servidor de mensajeria.
        Devuelve None si la conexion falla.

        :return: socket conectado o None
        """
        # intentamos conectar al servidor de mensajeria; si falla devolvemos None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((client._server, client._port))
            return sock
        except Exception:
            return None

    # ================= HILO DE ESCUCHA =================

    @staticmethod
    def _listener_thread(listen_sock, stop_event):
        """
        Funcion que ejecuta el hilo de escucha del cliente.
        Acepta conexiones entrantes del servidor (mensajes y ACKs)
        y las procesa segun el protocolo.

        Operaciones soportadas:
          - SEND_MESSAGE: mensaje sin adjunto del servidor (seccion 8.6)
          - SEND_MESS_ACK: confirmacion de entrega sin adjunto (seccion 8.6)
          - SEND_MESSAGE_ATTACH: mensaje con adjunto del servidor (parte 2 seccion 2.3)
          - SEND_MESS_ATTACH_ACK: confirmacion de entrega con adjunto (parte 2 seccion 2.3)
          - GET_FILE: solicitud de transferencia de fichero (parte 2 seccion 2.5)

        Se detiene cuando stop_event esta activado o cuando el socket de
        escucha se cierra.

        :param listen_sock: socket servidor (ya en estado listen) del cliente
        :param stop_event:  threading.Event para senalizar la parada
        """
        # ponemos timeout para poder comprobar el flag de parada periodicamente
        listen_sock.settimeout(1.0)
        while not stop_event.is_set():
            # esperamos una conexion entrante del servidor
            try:
                conn, _ = listen_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            # procesamos la conexion entrante segun la operacion recibida
            try:
                operacion = client._recv_line(conn)
                if operacion is None:
                    conn.close()
                    continue

                if operacion == "SEND_MESSAGE":
                    # mensaje sin adjunto: leemos remitente, id y texto (protocolo 8.6)
                    remitente = client._recv_line(conn)
                    id_str    = client._recv_line(conn)
                    texto     = client._recv_line(conn)
                    conn.close()

                    # mostramos el mensaje recibido (seccion 6.7)
                    print("\ns> MESSAGE {} FROM {}".format(id_str, remitente))
                    print("   {}".format(texto))
                    print("   END")
                    print("c> ", end="", flush=True)

                elif operacion == "SEND_MESS_ACK":
                    # confirmacion de entrega sin adjunto: leemos el id confirmado
                    id_str = client._recv_line(conn)
                    conn.close()

                    # notificamos al usuario que su mensaje fue entregado
                    print("\nc> SEND MESSAGE {} OK".format(id_str))
                    print("c> ", end="", flush=True)

                elif operacion == "SEND_MESSAGE_ATTACH":
                    # mensaje con adjunto: remitente, id, texto, filename
                    # (parte 2, seccion 2.3)
                    remitente = client._recv_line(conn)
                    id_str    = client._recv_line(conn)
                    texto     = client._recv_line(conn)
                    filename  = client._recv_line(conn)
                    conn.close()

                    # mostramos el mensaje con la ruta del adjunto (seccion 2.5 parte 2)
                    print("\nc> MESSAGE {} FROM {}".format(id_str, remitente))
                    print("   {}".format(texto))
                    print("   END")
                    print("   FILE {}".format(filename))
                    print("c> ", end="", flush=True)

                elif operacion == "SEND_MESS_ATTACH_ACK":
                    # confirmacion de entrega con adjunto: id y nombre de fichero
                    # (parte 2, seccion 2.3)
                    id_str   = client._recv_line(conn)
                    filename = client._recv_line(conn)
                    conn.close()

                    # notificamos al usuario que su mensaje con adjunto fue entregado
                    print("\nc> SENDATTACH MESSAGE {} {} OK".format(id_str, filename))
                    print("c> ", end="", flush=True)

                elif operacion == "GET_FILE":
                    # solicitud de transferencia de fichero de otro cliente
                    # (parte 2, seccion 2.5)
                    # protocolo: GET_FILE \n solicitante \n filename \n
                    # respuesta: contenido del fichero en crudo por el socket
                    _solicitante = client._recv_line(conn)
                    filename     = client._recv_line(conn)

                    # leemos el fichero y lo enviamos byte a byte por el socket
                    try:
                        with open(filename, "rb") as f:
                            datos = f.read()
                        conn.sendall(datos)
                    except Exception:
                        # si no se puede abrir el fichero enviamos vacio
                        pass

                    # shutdown SHUT_WR envia FIN TCP para que el receptor
                    # detecte el EOF y salga del bucle recv correctamente
                    try:
                        conn.shutdown(socket.SHUT_WR)
                    except Exception:
                        pass
                    conn.close()

                else:
                    # operacion desconocida: cerramos la conexion
                    conn.close()

            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass

        # al salir del bucle cerramos el socket de escucha
        try:
            listen_sock.close()
        except Exception:
            pass

    @staticmethod
    def _find_free_port():
        """
        Busca un puerto TCP libre en el sistema operativo asignandolo
        dinamicamente mediante bind en el puerto 0.

        :return: numero de puerto libre (int) o -1 si falla
        """
        # bind al puerto 0 hace que el SO asigne un puerto libre automaticamente
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("", 0))
            port = s.getsockname()[1]
            s.close()
            return port
        except Exception:
            return -1

    @staticmethod
    def _start_listener():
        """
        Busca un puerto libre, crea el socket de escucha del cliente,
        lo pone en estado listen y lanza el hilo de escucha.

        :return: puerto de escucha (int) o -1 si falla
        """
        # paso 1: buscamos un puerto libre para el hilo de escucha (figura 2)
        port = client._find_free_port()
        if port == -1:
            return -1

        # paso 2: creamos el socket de escucha y lo ponemos en modo listen
        try:
            listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listen_sock.bind(("", port))
            listen_sock.listen(10)
        except Exception:
            return -1

        # lanzamos el hilo de escucha con el evento de parada
        stop_event = threading.Event()
        t = threading.Thread(
            target=client._listener_thread,
            args=(listen_sock, stop_event),
            daemon=True
        )
        t.start()

        # guardamos referencias al socket, hilo y evento para poder pararlos en DISCONNECT
        client._listen_sock   = listen_sock
        client._listen_port   = port
        client._listen_thread = t
        client._stop_event    = stop_event

        return port

    @staticmethod
    def _stop_listener():
        """
        Detiene el hilo de escucha cerrando su socket y activando el evento
        de parada. Se llama en DISCONNECT o ante cualquier error de conexion.
        """
        # activamos el evento para que el hilo salga de su bucle
        if client._stop_event is not None:
            client._stop_event.set()

        # cerramos el socket para forzar la salida del accept bloqueante
        if client._listen_sock is not None:
            try:
                client._listen_sock.close()
            except Exception:
                pass
            client._listen_sock = None

        # esperamos a que el hilo termine limpiamente (timeout de seguridad)
        if client._listen_thread is not None:
            client._listen_thread.join(timeout=2.0)
            client._listen_thread = None

        # limpiamos el resto de atributos de estado
        client._listen_port    = -1
        client._stop_event     = None
        client._connected_user = None

    # ================= OPERACIONES DEL PROTOCOLO =================

    @staticmethod
    def register(user):
    
        # abrimos conexion al servidor para esta operacion
        sock = client._connect_to_server()
        if sock is None:
            print("c> REGISTER FAIL")
            return client.RC.ERROR

        # enviamos la operacion y el nombre de usuario, leemos el resultado
        try:
            client._send_line(sock, "REGISTER")
            client._send_line(sock, user)
            resp = client._recv_byte(sock)
        except Exception:
            resp = -1
        finally:
            sock.close()

        # interpretamos el codigo de respuesta del servidor (seccion 6.2)
        if resp == 0:
            print("c> REGISTER OK")
            return client.RC.OK
        elif resp == 1:
            print("c> USERNAME IN USE")
            return client.RC.USER_ERROR
        else:
            print("c> REGISTER FAIL")
            return client.RC.ERROR

    @staticmethod
    def unregister(user):
        
        # abrimos conexion al servidor para esta operacion
        sock = client._connect_to_server()
        if sock is None:
            print("c> UNREGISTER FAIL")
            return client.RC.ERROR

        # enviamos la operacion y el nombre de usuario, leemos el resultado
        try:
            client._send_line(sock, "UNREGISTER")
            client._send_line(sock, user)
            resp = client._recv_byte(sock)
        except Exception:
            resp = -1
        finally:
            sock.close()

        # interpretamos el codigo de respuesta del servidor (seccion 6.3)
        if resp == 0:
            print("c> UNREGISTER OK")
            return client.RC.OK
        elif resp == 1:
            print("c> USER DOES NOT EXIST")
            return client.RC.USER_ERROR
        else:
            print("c> UNREGISTER FAIL")
            return client.RC.ERROR

    @staticmethod
    def connect(user):
        
        # comprobamos que no haya ya un usuario conectado en este cliente
        if client._connected_user is not None:
            print("c> CONNECT FAIL")
            return client.RC.ERROR

        # paso 1 y 2 (figura 2): buscamos puerto libre y lanzamos el hilo de escucha
        port = client._start_listener()
        if port == -1:
            print("c> CONNECT FAIL")
            return client.RC.ERROR

        # paso 3 (figura 2): enviamos la solicitud de conexion al servidor
        sock = client._connect_to_server()
        if sock is None:
            client._stop_listener()
            print("c> CONNECT FAIL")
            return client.RC.ERROR

        # enviamos operacion, nombre y puerto de escucha del cliente
        try:
            client._send_line(sock, "CONNECT")
            client._send_line(sock, user)
            client._send_line(sock, str(port))
            resp = client._recv_byte(sock)
        except Exception:
            resp = -1
        finally:
            sock.close()

        # interpretamos el codigo de respuesta del servidor (seccion 6.4)
        if resp == 0:
            # exito: guardamos el nombre del usuario conectado
            client._connected_user = user
            print("c> CONNECT OK")
            return client.RC.OK
        elif resp == 1:
            # usuario no registrado: paramos el hilo de escucha
            client._stop_listener()
            print("c> CONNECT FAIL, USER DOES NOT EXIST")
            return client.RC.USER_ERROR
        elif resp == 2:
            # usuario ya conectado: paramos el hilo de escucha
            client._stop_listener()
            print("c> USER ALREADY CONNECTED")
            return client.RC.USER_ERROR
        else:
            # error generico: paramos el hilo de escucha
            client._stop_listener()
            print("c> CONNECT FAIL")
            return client.RC.ERROR

    @staticmethod
    def disconnect(user):
        """
        Desconecta al usuario del servicio de mensajeria.
        Protocolo seccion 8.4:
          -> DISCONNECT
          -> <userName>
          <- byte (0=OK, 1=no existe, 2=no conectado, 3=error)

        :param user: nombre de usuario que se desconecta
        :return: RC.OK, RC.USER_ERROR o RC.ERROR
        """
        # abrimos conexion al servidor para notificar la desconexion
        sock = client._connect_to_server()
        if sock is None:
            # aunque falle la comunicacion paramos el hilo (seccion 6.5)
            client._stop_listener()
            print("c> DISCONNECT FAIL")
            return client.RC.ERROR

        # enviamos la operacion y el nombre de usuario, leemos el resultado
        try:
            client._send_line(sock, "DISCONNECT")
            client._send_line(sock, user)
            resp = client._recv_byte(sock)
        except Exception:
            resp = -1
        finally:
            sock.close()

        # en cualquier caso paramos el hilo de escucha (seccion 6.5)
        client._stop_listener()

        # interpretamos el codigo de respuesta del servidor
        if resp == 0:
            print("c> DISCONNECT OK")
            return client.RC.OK
        elif resp == 1:
            print("c> DISCONNECT FAIL, USER DOES NOT EXIST")
            return client.RC.USER_ERROR
        elif resp == 2:
            print("c> DISCONNECT FAIL, USER NOT CONNECTED")
            return client.RC.USER_ERROR
        else:
            print("c> DISCONNECT FAIL")
            return client.RC.ERROR

    @staticmethod
    def send(user, message):
        """
        Envia un mensaje de texto SIN fichero adjunto a otro usuario.
        Protocolo seccion 8.5:
          -> SEND
          -> <remitente>
          -> <destinatario>
          -> <mensaje>
          <- byte (0=OK + id, 1=no existe, 2=error)

        :param user:    nombre del usuario destinatario
        :param message: texto del mensaje
        :return: RC.OK, RC.USER_ERROR o RC.ERROR
        """
        # solo podemos enviar si hay un usuario conectado
        if client._connected_user is None:
            print("c> SEND FAIL")
            return client.RC.ERROR

        # abrimos conexion al servidor para esta operacion
        sock = client._connect_to_server()
        if sock is None:
            print("c> SEND FAIL")
            return client.RC.ERROR

        # truncamos el mensaje si supera el maximo permitido (seccion 8.5)
        if len(message) > MAX_MSG_TEXT - 1:
            message = message[:MAX_MSG_TEXT - 1]

        # enviamos operacion, remitente, destinatario y mensaje; leemos respuesta
        try:
            client._send_line(sock, "SEND")
            client._send_line(sock, client._connected_user)
            client._send_line(sock, user)
            client._send_line(sock, message)
            resp = client._recv_byte(sock)
            # en caso de exito el servidor devuelve ademas el identificador
            if resp == 0:
                id_str = client._recv_line(sock)
            else:
                id_str = None
        except Exception:
            resp   = -1
            id_str = None
        finally:
            sock.close()

        # interpretamos el codigo de respuesta del servidor (seccion 6.6)
        if resp == 0:
            print("c> SEND OK - MESSAGE {}".format(id_str))
            return client.RC.OK
        elif resp == 1:
            print("c> SEND FAIL, USER DOES NOT EXIST")
            return client.RC.USER_ERROR
        else:
            print("c> SEND FAIL")
            return client.RC.ERROR

    @staticmethod
    def users():
        """
        Solicita al servidor la lista de usuarios actualmente conectados.
        Modificado para parte 2 (seccion 2.4): el servidor devuelve por cada
        usuario "nombre :: IP :: puerto". Esta informacion se almacena en
        _users_info para su uso posterior en GETFILE.

        Protocolo seccion 8.7 (modificado):
          -> USERS
          -> <userName>
          <- byte (0=OK, 1=no conectado, 2=error)
          <- <N>
          <- "nombre :: IP :: puerto" x N

        :return: RC.OK, RC.USER_ERROR o RC.ERROR
        """
        # solo podemos pedir usuarios si estamos conectados
        if client._connected_user is None:
            print("c> CONNECTED USERS FAIL")
            return client.RC.ERROR

        # abrimos conexion al servidor para esta operacion
        sock = client._connect_to_server()
        if sock is None:
            print("c> CONNECTED USERS FAIL")
            return client.RC.ERROR

        # enviamos la operacion y el nombre del usuario que hace la peticion
        try:
            client._send_line(sock, "USERS")
            client._send_line(sock, client._connected_user)
            resp = client._recv_byte(sock)

            # en caso de exito leemos el numero de conectados y sus entradas
            if resp == 0:
                n_str = client._recv_line(sock)
                n = int(n_str) if n_str is not None else 0
                entradas = []
                for _ in range(n):
                    entrada = client._recv_line(sock)
                    if entrada is not None:
                        entradas.append(entrada)
            else:
                n       = 0
                entradas = []
        except Exception:
            resp     = -1
            n        = 0
            entradas = []
        finally:
            sock.close()

        # interpretamos el codigo de respuesta del servidor (seccion 6.8)
        if resp == 0:
            print("c> CONNECTED USERS ({} users connected) OK".format(n))
            # parseamos y almacenamos la info de cada usuario conectado
            # formato de cada entrada: "nombre :: IP :: puerto"
            nueva_info = {}
            for entrada in entradas:
                print("   {}".format(entrada))
                partes = entrada.split(" :: ")
                if len(partes) == 3:
                    nombre_u = partes[0].strip()
                    ip_u     = partes[1].strip()
                    try:
                        puerto_u = int(partes[2].strip())
                    except ValueError:
                        puerto_u = -1
                    # guardamos ip y puerto del usuario para poder hacer GETFILE despues
                    nueva_info[nombre_u] = (ip_u, puerto_u)
            # actualizamos la estructura de datos de usuarios conectados con mutex
            with client._users_lock:
                client._users_info = nueva_info
            return client.RC.OK
        elif resp == 1:
            print("c> CONNECTED USERS FAIL, USER IS NOT CONNECTED")
            return client.RC.USER_ERROR
        else:
            print("c> CONNECTED USERS FAIL")
            return client.RC.ERROR

    @staticmethod
    def sendAttach(user, file, message):
        """
        Envia un mensaje CON fichero adjunto a otro usuario.
        Implementado para la Parte 2.

        Protocolo (seccion 2.2 parte 2):
          -> SENDATTACH
          -> <remitente>
          -> <destinatario>
          -> <mensaje>
          -> <filename>
          <- byte (0=OK + id, 1=no existe, 2=error)

        :param user:    nombre del usuario destinatario
        :param file:    ruta del fichero adjunto (path absoluto)
        :param message: texto del mensaje
        :return: RC.OK, RC.USER_ERROR o RC.ERROR
        """
        # solo podemos enviar si hay un usuario conectado
        if client._connected_user is None:
            print("c> SENDATTACH FAIL")
            return client.RC.ERROR

        # abrimos conexion al servidor para esta operacion
        sock = client._connect_to_server()
        if sock is None:
            print("c> SENDATTACH FAIL")
            return client.RC.ERROR

        # truncamos el mensaje si supera el maximo permitido
        if len(message) > MAX_MSG_TEXT - 1:
            message = message[:MAX_MSG_TEXT - 1]

        # truncamos el nombre de fichero si supera el maximo
        if len(file) > MAX_FILENAME - 1:
            file = file[:MAX_FILENAME - 1]

        # enviamos operacion, remitente, destinatario, mensaje y ruta del fichero
        try:
            client._send_line(sock, "SENDATTACH")
            client._send_line(sock, client._connected_user)
            client._send_line(sock, user)
            client._send_line(sock, message)
            client._send_line(sock, file)
            resp = client._recv_byte(sock)
            # en caso de exito el servidor devuelve ademas el identificador
            if resp == 0:
                id_str = client._recv_line(sock)
            else:
                id_str = None
        except Exception:
            resp   = -1
            id_str = None
        finally:
            sock.close()

        # interpretamos el codigo de respuesta del servidor
        if resp == 0:
            print("c> SENDATTACH OK - MESSAGE {}".format(id_str))
            return client.RC.OK
        elif resp == 1:
            print("c> SENDATTACH FAIL, USER DOES NOT EXIST")
            return client.RC.USER_ERROR
        else:
            print("c> SENDATTACH FAIL")
            return client.RC.ERROR

    @staticmethod
    def getFile(user, remote_filename, local_filename):
        """
        Solicita la transferencia de un fichero desde otro usuario conectado.
        (Parte 2, seccion 2.5)

        Primero busca la IP y puerto del usuario en _users_info. Si no lo
        encuentra, hace una peticion interna de USERS para refrescar la
        informacion. Si el usuario sigue sin encontrarse (desconectado),
        muestra el error correspondiente.

        Protocolo cliente->cliente:
          -> GET_FILE
          -> <solicitante>
          -> <filename>
          <- contenido del fichero (bytes hasta cierre del socket)

        El contenido recibido se escribe en local_filename.

        :param user:           nombre del usuario que posee el fichero
        :param remote_filename: ruta del fichero remoto
        :param local_filename:  ruta local donde guardar el fichero
        :return: RC.OK o RC.ERROR
        """
        # solo podemos hacer getfile si estamos conectados
        if client._connected_user is None:
            print("c> FILE TRANSFER FAILED, user not connected.")
            return client.RC.ERROR

        # buscamos la info del usuario en la estructura de datos local
        with client._users_lock:
            info = client._users_info.get(user, None)

        # si no la tenemos, refrescamos consultando al servidor
        if info is None:
            client.users()
            with client._users_lock:
                info = client._users_info.get(user, None)

        # si sigue sin aparecer, el usuario no esta conectado
        if info is None:
            print("c> FILE TRANSFER FAILED, user not connected.")
            return client.RC.ERROR

        ip_dest, puerto_dest = info

        # conectamos directamente al hilo de escucha del cliente remoto
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip_dest, puerto_dest))
        except Exception:
            print("c> FILE TRANSFER FAILED, user not connected.")
            return client.RC.ERROR

        # enviamos la solicitud de fichero y recibimos los bytes hasta EOF
        try:
            client._send_line(sock, "GET_FILE")
            client._send_line(sock, client._connected_user)
            client._send_line(sock, remote_filename)

            # acumulamos el contenido del fichero hasta que el remoto cierra el socket
            datos = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                datos += chunk
        except Exception as e:
            print("c> FILE TRANSFER FAILED: {}".format(e))
            sock.close()
            return client.RC.ERROR
        finally:
            sock.close()

        # guardamos el contenido recibido en el fichero local
        try:
            with open(local_filename, "wb") as f:
                f.write(datos)
            print("c> FILE TRANSFER OK")
            return client.RC.OK
        except Exception as e:
            print("c> FILE TRANSFER FAILED: {}".format(e))
            return client.RC.ERROR

    # ================= INTERFAZ DE USUARIO (SHELL) =================

    @staticmethod
    def shell():
        """
        Interprete de comandos del cliente.
        Lee comandos de la consola y llama a los metodos del protocolo.
        Comandos disponibles: REGISTER, UNREGISTER, CONNECT, DISCONNECT,
                              USERS, SEND, SENDATTACH, GETFILE, QUIT.
        """
        while True:
            try:
                command = input("c> ")
                line = command.split(" ")
                if len(line) > 0:

                    line[0] = line[0].upper()

                    if line[0] == "REGISTER":
                        if len(line) == 2:
                            client.register(line[1])
                        else:
                            print("Syntax error. Usage: REGISTER <userName>")

                    elif line[0] == "UNREGISTER":
                        if len(line) == 2:
                            client.unregister(line[1])
                        else:
                            print("Syntax error. Usage: UNREGISTER <userName>")

                    elif line[0] == "CONNECT":
                        if len(line) == 2:
                            client.connect(line[1])
                        else:
                            print("Syntax error. Usage: CONNECT <userName>")

                    elif line[0] == "DISCONNECT":
                        if len(line) == 2:
                            client.disconnect(line[1])
                        else:
                            print("Syntax error. Usage: DISCONNECT <userName>")

                    elif line[0] == "USERS":
                        if len(line) == 1:
                            client.users()
                        else:
                            print("Syntax error. Usage: USERS")

                    elif line[0] == "SEND":
                        if len(line) >= 3:
                            message = " ".join(line[2:])
                            client.send(line[1], message)
                        else:
                            print("Syntax error. Usage: SEND <userName> <message>")

                    elif line[0] == "SENDATTACH":
                        # Uso: SENDATTACH <userName> <message> <fileName>
                        # El fichero es el ultimo argumento, el mensaje es el resto
                        if len(line) >= 4:
                            dest_user = line[1]
                            filename  = line[-1]
                            message   = " ".join(line[2:-1])
                            client.sendAttach(dest_user, filename, message)
                        else:
                            print("Syntax error. Usage: SENDATTACH <userName> <message> <fileName>")

                    elif line[0] == "GETFILE":
                        # Uso: GETFILE <userName> <fileName> <localFileName>
                        if len(line) == 4:
                            client.getFile(line[1], line[2], line[3])
                        else:
                            print("Syntax error. Usage: GETFILE <userName> <fileName> <localFileName>")

                    elif line[0] == "QUIT":
                        if len(line) == 1:
                            break
                        else:
                            print("Syntax error. Use: QUIT")

                    else:
                        print("Error: command " + line[0] + " not valid.")

            except EOFError:
                break
            except Exception as e:
                print("Exception: " + str(e))

    # ================= ARGUMENTOS Y MAIN =================

    @staticmethod
    def usage():
        """Muestra el uso correcto del programa."""
        print("Usage: python3 client.py -s <server> -p <port>")

    @staticmethod
    def parseArguments(argv):
        """
        Parsea los argumentos de linea de comandos.
        Requiere -s <IP_servidor> y -p <puerto>.

        :param argv: lista de argumentos (normalmente sys.argv)
        :return: True si los argumentos son validos, False en caso contrario
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("-s", type=str, required=True, help="Server IP")
        parser.add_argument("-p", type=int, required=True, help="Server Port")
        args = parser.parse_args()

        if args.s is None:
            parser.error("Usage: python3 client.py -s <server> -p <port>")
            return False

        # validamos que el puerto este en el rango permitido
        if args.p < 1024 or args.p > 65535:
            parser.error("Error: Port must be in the range 1024 <= port <= 65535")
            return False

        # guardamos la IP y puerto del servidor como atributos de clase
        client._server = args.s
        client._port   = args.p
        return True

    @staticmethod
    def main(argv):
        """
        Punto de entrada principal del cliente.
        Parsea argumentos, lanza el shell interactivo y termina.

        :param argv: argumentos del programa
        """
        if not client.parseArguments(argv):
            client.usage()
            return

        # lanzamos el interprete de comandos interactivo
        client.shell()
        print("+++ FINISHED +++")


# ===================== PUNTO DE ENTRADA =====================

if __name__ == "__main__":
    client.main(sys.argv)