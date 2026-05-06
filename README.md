#Servicio de mensajeria-Practica Final UC3M

#Para la compilacion hay que ejecutarel Makefile con lo siguiente:

make clean
make

#Servidor
./server -p 8080

Al arrancar mostrara :
s> init server 192.168.1.181:8080

siendo la IP la real de la maquina del servidor en la red local a la que este conectada

#Cliente
python3 client.py -s <IP_servidor> -p <puerto_servidor>

#Ejemplos de ejecucion:

- Si cliente y servidor están en la misma máquina:
  python3 client.py -s localhost -p 8080

- Si están en máquinas o contenedores distintos:
  python3 client.py -s 192.168.1.181 -p 8888

#Comandos disponibles por el cliente

REGISTER <userName>   - Registrar usuario
UNREGISTER <userName> - Dar de baja usuario
CONNECT <userName>   - Conectarse al servicio
DISCONNECT <userName>  - Desconectarse del servicio
USERS  - Ver usuarios conectados
SEND <userName> <mensaje> - Enviar mensaje
SENDATTACH <userName> <mensaje> <fichero> - Enviar mensaje con adjunto
GETFILE <userName> <ficheroRemoto> <ficheroLocal> - Descargar fichero del usario que quieres cogerle el fichero
QUIT     - Salir