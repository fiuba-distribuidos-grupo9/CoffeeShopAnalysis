<br>
<p align="center">
  <img src="https://huergo.edu.ar/images/convenios/fiuba.jpg" width="100%" style="background-color:white"/>
</p>

# ☕ Coffee Shop Analysis

## 📚 Materia: Sistemas Distribuidos 1 (Roca)

## 👥 Grupo 9

### Integrantes

| Nombre                                                          | Padrón |
| --------------------------------------------------------------- | ------ |
| [Ascencio Felipe Santino](https://github.com/FelipeAscencio)    | 110675 |
| [Gamberale Luciano Martín](https://github.com/lucianogamberale) | 105892 |
| [Zielonka Axel](https://github.com/axel-zielonka)               | 110310 |

### Corrector

- [Franco Papa](https://github.com/F-Papa)

## 📖 Descripción (Health Checkers - Topología de Anillo)

El sistema de **Health checkers** en anillo implementado, nos permite validar el estado actual de los nodos del sistema.

Además de darnos la posibilidad de volver a levantar nodos caídos de forma automática.

- **UDP ring** con **heartbeats** al sucesor para detectar caídas.
- **Reacomodo** del anillo al detectar un nodo caído (salteo del sucesor).
- **Elección de líder (Chang–Roberts)** si cae el líder o no se conoce.
- **Revive automático** con **DooD** (mismo espíritu que `healther`): mapeo `NODE_NAME -> CONTAINER_NAME` vía `REVIVE_TARGETS`.
- **Loop del líder** con **sleep aleatorio** (placeholder para el “ping global” futuro). **Solo el líder** ejecuta ese bucle.

## Variables de entorno

Ver `.env.example`. Lo mínimo:

- `NODE_ID`, `NODE_NAME`, `LISTEN_PORT`.
- `RING_PEERS` = `id@host:port,...` (Es importante no incluirse a sí mismo).
- `REVIVE_TARGETS` = `nodeName=containerName,...`.
- `MODE=auto|manual`.

## Tutorial de uso

### 🧱 Build de la imagen

```bash

make build

```

### 🐳 Script de prueba (3 nodos)

```bash

make up

```

Luego para detener y borrar esos contenedores de forma rápida se puede utilizar el siguiente comando.

```bash

make down

```

### 🐳 Ver logs

```bash

make logs$NUMERO_DE_HC

```

#### Ejemplo

```bash

make logs3

```

### 🐳 Tirar un nodo

```bash

make stop$NUMERO_DE_HC

```

#### Ejemplo

```bash

make stop3

```
