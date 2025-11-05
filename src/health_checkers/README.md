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

docker build -t tp-health-checker -f Dockerfile .

```

### 🐳 Ejecutarlos en sus respectivos contenedores

```bash

docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=$NUMERO_DE_NODO \
  -e NODE_NAME=hc-$NUMERO_DE_NODO \
  -e LISTEN_PORT=$PUERTO_LOCAL \
  -e RING_PEERS="$ANTERIOR_NODO@$HOST_ANTERIOR:$PUERTO_ANTERIOR,$SIGUIENTE_NODO@$HOST_SIGUIENTE:$PUERTO_SIGUIENTE" \
  -e MODE=auto \
  tp-health-checker

```

#### Ejemplo

```bash

# Nodo 1.
docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=1 \
  -e NODE_NAME=hc-1 \
  -e LISTEN_PORT=9101 \
  -e RING_PEERS="2@127.0.0.1:9102,3@127.0.0.1:9103" \
  -e MODE=auto \
  tp-health-checker

# Nodo 2.
docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=2 \
  -e NODE_NAME=hc-2 \
  -e LISTEN_PORT=9102 \
  -e RING_PEERS="1@127.0.0.1:9101,3@127.0.0.1:9103" \
  -e MODE=auto \
  tp-health-checker

# Nodo 3.
docker run --rm -it \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e NODE_ID=3 \
  -e NODE_NAME=hc-3 \
  -e LISTEN_PORT=9103 \
  -e RING_PEERS="1@127.0.0.1:9101,2@127.0.0.1:9102" \
  -e MODE=auto \
  tp-health-checker

```

