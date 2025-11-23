ToDo list:
- cuando ahora partimos cada mensaje en por ej el user_cleaners, vamos a generar un problema si se cae justo cuando se envio uno, a menos que los guardemos antes de enviarlos por si justo se cae ahi (como haceoms en los nodos statefull). La otra es que shardeemos por usuario, y con esto nos sacamos el problema de encima, pero si tenemos mucha mala suerte podria quedar todo super desbalanceado. Tambien nos va a pasar para los mensajes que duplicamos a mano, que ahora se estan mandando dos mensajes con el mismo id. Creo que esto no va a generar problemas igual porque se usa como info base o la info termina en queries distintas y nunca va conflictuar.  
- cada controller tiene que guardar el ultimo mensaje que le mando el anterior controller (segun el id). No se si convendra hacer eso para los eof. 

Annotations:
- preferimos mandarle dos veces a alguien el mensaje
- después guardamos el estado que teníamos -> en los stateless no sé si tiene sentido??? 


- el server le debería agregar a cada mensaje un uuid (capaz de paso también puede tener la responsabilidad de agregar lo del cliente para que quede mas prolijo)
- en cuanto se cae el server tiene que mandar un mensaje a todos para que limpien la información de todos los clientes.
- para limpiar todo, el server debería esperar que pase todo el mensaje de limpieza por todos los controllers y llegue como resultado. 
- podria hacer que la queue de resultados sea "efimera" para que se borre automaticamente si se vuela el server, porque sino potencialmente me podrian quedar muchas
queues ahi.
- ahora deberíamos ver de qué controller vino el EOF, porque pueden venir duplicados, y deberíamos tener cuidado de cómo los contamos
