ToDo list (backlog):
- cada controller tiene que limpiar todo lo referido a la session y pasar el mensaje a todos los siguientes

- use _log functions on controllers
- deberiamos imprimir lo que se va cargando de la metadata, total es una unica vez que se hace
- implementar la espera del joiner de una mejor manera (capaz mandando un mensaje especial, porque potencialmente podría quedar en un loop infinito o quedar basura)
- las sections del archivo que usan json son todas iguales, se pueden unificar en una jerarquia. 
- ver que se tiro un sigterm de la nada en el cliente o server, puede explotar porque cierro el socket, y termina con un exit(1)
- podemos dividir cuanto se le manda a cada lado de los controllers en los que se bifurca
- aplicar patrón visitor
- limpiar los @TODO

Annotations:
- preferimos mandarle dos veces a alguien el mensaje
- después guardamos el estado que teníamos
