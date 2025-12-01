ToDo list:
- deberiamos imprimir lo que se va cargando de la metadata, total es una unica vez que se hace
- tener cuidado con los statefull de primero guardar todo en un archivo y recien despues enviar, porque sino vamos a generar mensajes de mas que no se van a poder detectar como duplicados.
- cada controller tiene que guardar el ultimo mensaje que le mando el anterior controller (segun el id). No se si convendra hacer eso para los eof. 
- en cuanto se cae el server tiene que mandar un mensaje a todos para que limpien la información de todos los clientes.
- aplicar patrón visitor
- actualizar el rabbit para que persista siempre los mensajes

Annotations:
- preferimos mandarle dos veces a alguien el mensaje
- después guardamos el estado que teníamos -> en los stateless no sé si tiene sentido??? 


- para limpiar todo, el server debería esperar que pase todo el mensaje de limpieza por todos los controllers y llegue como resultado. 
- podria hacer que la queue de resultados sea "efimera" para que se borre automaticamente si se vuela el server, porque sino potencialmente me podrian quedar muchas
queues ahi.
- ahora deberíamos ver de qué controller vino el EOF, porque pueden venir duplicados, y deberíamos tener cuidado de cómo los contamos
