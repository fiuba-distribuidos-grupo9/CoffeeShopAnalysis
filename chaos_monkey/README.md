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

## 📖 Descripción

En este directorio se encuentra la implementación de la herramienta 'Chaos-Monkey' para generar errores en los nodos del sistema distribuido 'Coffee Shop Analysis' en el segundo cuatrimestre del año 2025 en la materia 'Sistemas Distribuidos 1 (Roca)'.

## 🧰 Guía rápida de uso con Makefile

### 🐵 Chaos-Monkey

#### ¿Cómo configurarlo?

El modo manual lee los targets desde 'src/.env' y asigna números a cada contenedor (1..n).

En el mismo directorio se cuenta con un '.env.example', usar de referencia para armar las configuraciones deseadas.

#### Inspeccionar la configuración actual de forma rápida

```bash

make eligible

```

#### 🧪 Ejecutar el Chaos-Monkey (Modo manual)

```bash

make cm

```

#### 🤖 Ejecutar el Chaos-Monkey (Modo automático)

El modo automático usa el mismo '.env' y cada CHAOS_INTERVAL segundos intenta matar uno de los contenedores elegibles que esté “running”. Si el que elige ya está caído, lo reporta y busca otro.

```bash

make cm-auto

```

🛑 Se corta con 'Ctrl+C'.
