ToDo list:
- all EOF should be send with 0 uuid, the flush too, because it is identified by other things. 
- change controller ids on each controller
- sharding on each controller
- remove exchanges -> hello queues

Annotations:
- preferimos mandarle dos veces a alguien el mensaje
- después guardamos el estado que teníamos -> en los stateless no sé si tiene sentido??? 


- el server le debería agregar a cada mensaje un uuid (capaz de paso también puede tener la responsabilidad de agregar lo del cliente para que quede mas prolijo)
- en cuanto se cae el server tiene que mandar un mensaje a todos para que limpien la información de todos los clientes.
- para limpiar todo, el server debería esperar que pase todo el mensaje de limpieza por todos los controllers y llegue como resultado. 
- podria hacer que la queue de resultados sea "efimera" para que se borre automaticamente si se vuela el server, porque sino potencialmente me podrian quedar muchas
queues ahi.
- ahora deberíamos ver de qué controller vino el EOF, porque pueden venir duplicados, y deberíamos tener cuidado de cómo los contamos
