ToDo list (backlog):

- deberiamos imprimir lo que se va cargando de la metadata, total es una unica vez que se hace
- volar el requirements.txt
- [VER] implementar la espera del joiner de una mejor manera (capaz mandando un mensaje especial, porque potencialmente podría quedar en un loop infinito o quedar basura)
- podemos dividir cuanto se le manda a cada lado de los controllers en los que se bifurca
- aplicar patrón visitor
- las sections del archivo que usan json son todas iguales, se pueden unificar en una jerarquia. 
- limpiar los @TODO
- [VER] ver que se tiro un sigterm de la nada en el cliente o server, puede explotar porque cierro el socket, y termina con un exit(1)
- [VER] poner las dependencias de borrado en el compose porque sino se está explotando todo porque se cierrar queues mientras mandamos mensajes