"""
client.py - Cliente de mensajería (Parte 1)
Sistemas Distribuidos - UC3M

Cliente concurrente multihilo que gestiona:
 - Registro/baja de usuarios
 - Conexion/desconexion al servicio de mensajeria
 - Envio de mensajes a otros usuarios
 - Recepcion de mensajes del servidor (hilo de escucha)
 - Listado de usuarios conectados

Protocolo: sockets TCP, cadenas terminadas en '\n' (leidas con readline),
           un byte de respuesta del servidor.
Uso: python3 client.py -s <IP_servidor> -p <puerto_servidor>
"""

import socket
import threading
import argparse
import sys

# ===================== CONSTANTES =====================
MAX_NAME     = 256   # longitud maxima del nombre de usuario
MAX_MSG_TEXT = 256   # longitud maxima del texto de un mensaje

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
    _listen_sock   = None    # socket servidor del hilo de escucha
    _listen_port   = -1      # puerto en el que escucha el hilo de escucha
    _listen_thread = None    # hilo de escucha de mensajes entrantes
    _connected_user = None   # nombre del usuario actualmente conectado
    _stop_event    = None    # evento para detener el hilo de escucha

    # ================= UTILIDADES DE RED =================

    @staticmethod
    def _send_line(sock, texto):
        """
        Envia una cadena de texto por el socket seguida de un salto de linea '\n'.
        El servidor lee lineas hasta '\n' o '\0'.

        :param sock:  socket TCP ya conectado
        :param texto: cadena de texto a enviar (sin terminador)
        """
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
        buf = b""
        while True:
            ch = sock.recv(1)
            if not ch:
                # conexion cerrada por el otro extremo
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
        y las procesa segun el protocolo de la seccion 8.6.

        Se detiene cuando stop_event esta activado o cuando el socket de
        escucha se cierra.

        :param listen_sock: socket servidor (ya en estado listen) del cliente
        :param stop_event:  threading.Event para senalizar la parada
        """
        listen_sock.settimeout(1.0)  # timeout para poder comprobar stop_event
        while not stop_event.is_set():
            try:
                conn, _ = listen_sock.accept()
            except socket.timeout:
                # sin conexion entrante, comprobamos si hay que parar
                continue
            except OSError:
                # el socket fue cerrado externamente (DISCONNECT)
                break

            # leemos la operacion que envia el servidor
            try:
                operacion = client._recv_line(conn)
                if operacion is None:
                    conn.close()
                    continue

                if operacion == "SEND_MESSAGE":
                    # protocolo seccion 8.6: recibimos remitente, id y texto
                    remitente = client._recv_line(conn)
                    id_str    = client._recv_line(conn)
                    texto     = client._recv_line(conn)
                    conn.close()

                    # mostramos el mensaje recibido segun seccion 6.7
                    print("\ns> MESSAGE {} FROM {}".format(id_str, remitente))
                    print("   {}".format(texto))
                    print("   END")
                    # reimprimimos el prompt para no dejar la consola en blanco
                    print("c> ", end="", flush=True)

                elif operacion == "SEND_MESS_ACK":
                    # protocolo seccion 8.6 parte 2: confirmacion de entrega
                    id_str = client._recv_line(conn)
                    conn.close()

                    # mostramos confirmacion segun seccion 6.6
                    print("\nc> SEND MESSAGE {} OK".format(id_str))
                    print("c> ", end="", flush=True)

                else:
                    # operacion desconocida, ignoramos
                    conn.close()

            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass

        # cerramos el socket de escucha al terminar el hilo
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
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("", 0))          # SO asigna un puerto libre
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
        port = client._find_free_port()
        if port == -1:
            return -1

        try:
            listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listen_sock.bind(("", port))
            listen_sock.listen(10)
        except Exception:
            return -1

        # evento de parada y hilo de escucha
        stop_event = threading.Event()
        t = threading.Thread(
            target=client._listener_thread,
            args=(listen_sock, stop_event),
            daemon=True
        )
        t.start()

        # guardamos el estado en los atributos de clase
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
        if client._stop_event is not None:
            client._stop_event.set()

        if client._listen_sock is not None:
            try:
                client._listen_sock.close()
            except Exception:
                pass
            client._listen_sock = None

        if client._listen_thread is not None:
            client._listen_thread.join(timeout=2.0)
            client._listen_thread = None

        client._listen_port   = -1
        client._stop_event    = None
        client._connected_user = None

    # ================= OPERACIONES DEL PROTOCOLO =================

    @staticmethod
    def register(user):
        """
        Registra un nuevo usuario en el sistema de mensajeria.
        Protocolo seccion 8.1:
          -> REGISTER
          -> <userName>
          <- byte (0=OK, 1=ya existe, 2=error)

        :param user: nombre de usuario a registrar
        :return: RC.OK, RC.USER_ERROR o RC.ERROR
        """
        sock = client._connect_to_server()
        if sock is None:
            print("c> REGISTER FAIL")
            return client.RC.ERROR

        try:
            client._send_line(sock, "REGISTER")
            client._send_line(sock, user)
            resp = client._recv_byte(sock)
        except Exception:
            resp = -1
        finally:
            sock.close()

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
        """
        Da de baja a un usuario del sistema de mensajeria.
        Protocolo seccion 8.2:
          -> UNREGISTER
          -> <userName>
          <- byte (0=OK, 1=no existe, 2=error)

        :param user: nombre de usuario a dar de baja
        :return: RC.OK, RC.USER_ERROR o RC.ERROR
        """
        sock = client._connect_to_server()
        if sock is None:
            print("c> UNREGISTER FAIL")
            return client.RC.ERROR

        try:
            client._send_line(sock, "UNREGISTER")
            client._send_line(sock, user)
            resp = client._recv_byte(sock)
        except Exception:
            resp = -1
        finally:
            sock.close()

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
        """
        Conecta al usuario al servicio de mensajeria.
        Segun seccion 6.4:
          1. Busca un puerto libre.
          2. Lanza el hilo de escucha en ese puerto.
          3. Envia la solicitud de conexion al servidor.
        Protocolo seccion 8.3:
          -> CONNECT
          -> <userName>
          -> <puerto> (como cadena)
          <- byte (0=OK, 1=no existe, 2=ya conectado, 3=error)

        :param user: nombre de usuario que se conecta
        :return: RC.OK, RC.USER_ERROR o RC.ERROR
        """
        if client._connected_user is not None:
            print("c> CONNECT FAIL")
            return client.RC.ERROR
        # (1) buscamos puerto libre y (2) lanzamos el hilo de escucha
        port = client._start_listener()
        if port == -1:
            print("c> CONNECT FAIL")
            return client.RC.ERROR

        # (3) enviamos la solicitud de conexion al servidor
        sock = client._connect_to_server()
        if sock is None:
            client._stop_listener()
            print("c> CONNECT FAIL")
            return client.RC.ERROR

        try:
            client._send_line(sock, "CONNECT")
            client._send_line(sock, user)
            client._send_line(sock, str(port))
            resp = client._recv_byte(sock)
        except Exception:
            resp = -1
        finally:
            sock.close()

        if resp == 0:
            client._connected_user = user
            print("c> CONNECT OK")
            return client.RC.OK
        elif resp == 1:
            client._stop_listener()
            print("c> CONNECT FAIL, USER DOES NOT EXIST")
            return client.RC.USER_ERROR
        elif resp == 2:
            client._stop_listener()
            print("c> USER ALREADY CONNECTED")
            return client.RC.USER_ERROR
        else:
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
        Segun seccion 6.5, el hilo de escucha se para siempre,
        incluso si la operacion falla.

        :param user: nombre de usuario que se desconecta
        :return: RC.OK, RC.USER_ERROR o RC.ERROR
        """
        sock = client._connect_to_server()
        if sock is None:
            # servidor caido: paramos el hilo igualmente (seccion 6.5)
            client._stop_listener()
            print("c> DISCONNECT FAIL")
            return client.RC.ERROR

        try:
            client._send_line(sock, "DISCONNECT")
            client._send_line(sock, user)
            resp = client._recv_byte(sock)
        except Exception:
            resp = -1
        finally:
            sock.close()

        # siempre paramos el hilo de escucha (seccion 6.5)
        client._stop_listener()

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
        Envia un mensaje de texto a otro usuario.
        Protocolo seccion 8.5:
          -> SEND
          -> <remitente>   (nombre del usuario que envia)
          -> <destinatario>
          -> <mensaje>
          <- byte (0=OK, 1=usuario no existe, 2=error)
          <- <id>  (solo si byte == 0)

        El remitente es el usuario actualmente conectado.

        :param user:    nombre del usuario destinatario
        :param message: texto del mensaje (maximo 255 caracteres)
        :return: RC.OK, RC.USER_ERROR o RC.ERROR
        """
        # el remitente debe ser el usuario conectado en este momento
        if client._connected_user is None:
            print("c> SEND FAIL")
            return client.RC.ERROR

        sock = client._connect_to_server()
        if sock is None:
            print("c> SEND FAIL")
            return client.RC.ERROR

        # truncamos el mensaje si supera el maximo permitido (255 caracteres)
        if len(message) > MAX_MSG_TEXT - 1:
            message = message[:MAX_MSG_TEXT - 1]

        try:
            client._send_line(sock, "SEND")
            client._send_line(sock, client._connected_user)
            client._send_line(sock, user)
            client._send_line(sock, message)
            resp = client._recv_byte(sock)
            if resp == 0:
                id_str = client._recv_line(sock)
            else:
                id_str = None
        except Exception:
            resp   = -1
            id_str = None
        finally:
            sock.close()

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
        Protocolo seccion 8.7:
          -> USERS
          -> <userName>  (nombre del usuario que hace la peticion)
          <- byte (0=OK, 1=no conectado, 2=error)
          <- <N>         (numero de usuarios, solo si byte == 0)
          <- <user1>
          <- ...
          <- <userN>

        :return: RC.OK, RC.USER_ERROR o RC.ERROR
        """
        # el usuario que consulta debe estar conectado
        if client._connected_user is None:
            print("c> CONNECTED USERS FAIL")
            return client.RC.ERROR

        sock = client._connect_to_server()
        if sock is None:
            print("c> CONNECTED USERS FAIL")
            return client.RC.ERROR

        try:
            client._send_line(sock, "USERS")
            client._send_line(sock, client._connected_user)
            resp = client._recv_byte(sock)

            if resp == 0:
                # recibimos el numero de conectados
                n_str = client._recv_line(sock)
                n = int(n_str) if n_str is not None else 0
                # recibimos los nombres de los usuarios conectados
                nombres = []
                for _ in range(n):
                    nombre = client._recv_line(sock)
                    if nombre is not None:
                        nombres.append(nombre)
            else:
                n      = 0
                nombres = []
        except Exception:
            resp    = -1
            n       = 0
            nombres = []
        finally:
            sock.close()

        if resp == 0:
            print("c> CONNECTED USERS ({} users connected) OK".format(n))
            for nombre in nombres:
                print("   {}".format(nombre))
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
        Envia un mensaje con archivo adjunto (funcionalidad de la Parte 2).
        En la Parte 1 no se implementa; se muestra un aviso al usuario.

        :param user:    nombre del usuario destinatario
        :param file:    ruta del archivo adjunto
        :param message: texto del mensaje
        :return: RC.ERROR (no implementado en Parte 1)
        """
        print("c> SENDATTACH not implemented in Part 1")
        return client.RC.ERROR

    # ================= INTERFAZ DE USUARIO (SHELL) =================

    @staticmethod
    def shell():
        """
        Interprete de comandos del cliente.
        Lee comandos de la consola y llama a los metodos del protocolo.
        Comandos disponibles: REGISTER, UNREGISTER, CONNECT, DISCONNECT,
                              USERS, SEND, SENDATTACH, QUIT.
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
                        if len(line) >= 4:
                            message = " ".join(line[3:])
                            client.sendAttach(line[1], line[2], message)
                        else:
                            print("Syntax error. Usage: SENDATTACH <userName> <filename> <message>")

                    elif line[0] == "QUIT":
                        if len(line) == 1:
                            break
                        else:
                            print("Syntax error. Use: QUIT")

                    else:
                        print("Error: command " + line[0] + " not valid.")

            except EOFError:
                # stdin cerrado (p.ej. en pruebas automaticas)
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

        if args.p < 1024 or args.p > 65535:
            parser.error("Error: Port must be in the range 1024 <= port <= 65535")
            return False

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

        client.shell()
        print("+++ FINISHED +++")


# ===================== PUNTO DE ENTRADA =====================

if __name__ == "__main__":
    client.main(sys.argv)