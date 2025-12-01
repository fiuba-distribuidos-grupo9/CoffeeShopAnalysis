ToDo list (backlog):
- hacer que el server guarde las sesiones que va teniendo
- en cuanto se cae el server tiene que mandar un mensaje a todos para que limpien la información de todos los clientes (lo mismo pasa cuando se cae un cliente)
- que el controller tenga dos procesos, uno escuchando el heartbeat y el otro haciendo lo de siempre, ambos controlados (lo mismo para el server)
- actualizar el rabbit para que persista siempre los mensajes

- deberiamos imprimir lo que se va cargando de la metadata, total es una unica vez que se hace
- implementar la espera del joiner de una mejor manera (capaz mandando un mensaje especial, porque potencialmente podría quedar en un loop infinito o quedar basura)
- las sections del archivo que usan json son todas iguales, se pueden unificar en una jerarquia. 
- podemos dividir cuanto se le manda a cada lado de los controllers en los que se bifurca
- aplicar patrón visitor
- limpiar los @TODO

Annotations:
- preferimos mandarle dos veces a alguien el mensaje
- después guardamos el estado que teníamos
