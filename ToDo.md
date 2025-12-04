ToDo list (backlog):

- deberiamos imprimir lo que se va cargando de la metadata, total es una unica vez que se hace
- [VER] implementar la espera del joiner de una mejor manera (capaz mandando un mensaje especial, porque potencialmente podría quedar en un loop infinito o quedar basura)
- aplicar patrón visitor
- limpiar los @TODO
- [VER] ver que se tiro un sigterm de la nada en el cliente o server, puede explotar porque cierro el socket, y termina con un exit(1)
- [VER] poner las dependencias de borrado en el compose porque sino se está explotando todo porque se cierrar queues mientras mandamos mensajes