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

Este repositorio contiene el material del TP del sistema distribuido "Coffee Shop Analysis", correspondiente al segundo cuatrimestre del año 2025 en la materia Sistemas Distribuidos 1 (Roca).

## 🛠️ Informe de Diseño

El informe técnico incluye:
- Decisiones de diseño.
- Implementación de cada ejercicio.
- Protocolo de comunicación.
- Mecanismos de concurrencia utilizados.
- Instrucciones de ejecución.
- Mecanismos de control de fallas.
- Herramienta de creación de fallas.

[📑 Acceso al informe](./docs/Informe-G9-Diseño.pdf).

## 🧰 Guía rápida de uso con `Makefile`

### 🚀 Ejecución del Sistema con Docker Compose

En este TP se utiliza **Docker Compose** para levantar todos los componentes del sistema distribuido:
- El **cliente** que realiza las queries.
- El **servidor** que las recibe.
- Todos los **nodos** del sistema distribuido.
- El **middleware**.

#### ⚙️ Construir las imágenes Docker

Antes de levantar el sistema, es posible (aunque no obligatorio) construir manualmente todas las imágenes Docker definidas dentro del proyecto.

```bash

make docker-build-image

```

🔧 Este comando busca automáticamente todos los Dockerfile dentro de src/, los construye y los etiqueta con el nombre del directorio correspondiente.

💡 No es necesario ejecutarlo manualmente, ya que make docker-compose-up lo ejecuta automáticamente antes de levantar los contenedores.

#### ▶️ Levantar todo el sistema

```bash

make docker-compose-up

```

✅ Este comando pone en marcha todos los servicios del sistema distribuido (cliente, servidor, nodos y middleware).

Además, se asegura de reconstruir imágenes si detecta cambios y elimina contenedores huérfanos de ejecuciones anteriores.

#### ⏹️ Apagar todo el sistema

```bash

make docker-compose-down

```

❌ Detiene todos los servicios activos y elimina los contenedores, liberando los recursos utilizados.

El sistema quedará completamente detenido y en un estado limpio.

#### 📜 Ver los logs del sistema

```bash

make docker-compose-logs

```

👀 Muestra en consola los últimos 500 registros de cada contenedor y mantiene el seguimiento en tiempo real (-f).

Ideal para monitorear el comportamiento de los componentes durante la ejecución.

#### 🔎 Filtrar logs de un contenedor específico

```bash

make docker-compose-logs | grep '<nombre_del_contenedor>'

```

👉 Permite filtrar los logs para enfocarse en un componente en particular, por ejemplo los filters del sistema.

##### Ejemplo

```bash

make docker-compose-logs | grep 'filter'

```

### 🧪 Testing

El Makefile incluye un conjunto de herramientas de testing para verificar el correcto funcionamiento del sistema distribuido.

#### 🧱 Tests unitarios de funcionamiento del Middleware

```bash

make unit-tests

```

🔍 Ejecuta los tests unitarios definidos con pytest en modo detallado (--verbose).

Estos tests suelen enfocarse en el middleware u otras partes específicas del sistema.

##### 🧩 Consideración

Los **tests unitarios** se ejecutan siempre dentro del entorno de desarrollo basado en **Dev Containers**.  

Este enfoque garantiza un ambiente de ejecución **aislado, reproducible y controlado**, evitando inconsistencias entre configuraciones locales.  

Podés consultar más información sobre Dev Containers en la documentación oficial de Visual Studio Code:  

🔗 [https://code.visualstudio.com/docs/devcontainers/containers](https://code.visualstudio.com/docs/devcontainers/containers)

Para ejecutar correctamente estos tests, es necesario realizar una pequeña modificación previa:

1. Accedé al archivo `docker-compose-dev.yaml` ubicado dentro del directorio `.devcontainer/`.
2. **Descomentá las líneas correspondientes al servicio de RabbitMQ** destinado al entorno de pruebas.
3. Al hacerlo, se levantará **una instancia independiente de RabbitMQ** utilizada exclusivamente para la ejecución de los tests unitarios dentro del contenedor de desarrollo.

Estas líneas permanecen **comentadas por defecto** para evitar conflictos o sobrecargas con el **RabbitMQ principal** que se utiliza durante la ejecución normal del sistema distribuido.

De este modo, se evita que las pruebas interfieran con los procesos del sistema en funcionamiento o afecten el rendimiento general.

Esta configuración permite que los tests unitarios del middleware se ejecuten en un entorno completamente controlado,  
logrando un **nivel óptimo de aislamiento y fiabilidad**, y asegurando que los resultados de las pruebas reflejen con precisión el comportamiento del middleware sin depender del estado del sistema completo.

#### 🔗 Tests de integración

```bash

make integration-tests EXPECTED_VARIANT=<output_a_validar>

```

🧩 Este comando ejecuta el conjunto de tests de integración, comparando las salidas generadas por el sistema contra los resultados esperados.

El proceso incluye los siguientes pasos:

1. Copia y normaliza (mediante ordenamiento) los archivos generados por el sistema en '.results/query_results/'.

2. Guarda los resultados actuales en 'integration-tests/data/query_results/'.

3. Compara los resultados normalizados con los outputs esperados definidos en:

```bash

integration-tests/data/expected_output/<EXPECTED_VARIANT>/

```

4. Reporta las diferencias detectadas (si es que las hay) para cada consulta.

Observación: Los tests de integración deben de ser ejecutados luego de haber utilizado el Sistema Distribuido con un único cliente.

Esto para garantizar la correcta validación de la respuesta generada a cada una de las consultas de un único usuario.

##### Ejemplo con 'reduced_data'

```bash

make integration-tests

```

Observación: No hace falta asignar la variable, se pasa el valor 'reduced_data' por defecto.

##### Ejemplo con 'full_data'

```bash

make integration-tests EXPECTED_VARIANT=full_data

```

#### 🧪 Tests de propagación EOF

El sistema cuenta con una batería de tests para verificar la correcta propagación de los EOF entre los nodos, asegurando un cierre coordinado del flujo de datos en escenarios multi-cliente.

##### Paso 1 - Exportar logs

```bash

make docker-export-logs

```

📂 Este comando genera un directorio 'logs/' en el cual se almacenan los logs de cada servicio que contengan el término eof.

Cada archivo tiene el formato 'logs/<servicio>.log' y permite validar el flujo de finalización de datos en los componentes distribuidos.

Una vez hecho este paso se debe realizar una ejecución completa de uno o mas clientes en el Sistema Distribuido. Para luego avanzar al siguiente paso.

##### Paso 2 - Ejecutar validación de EOF

```bash

make test-all-eof-received

```

✅ Ejecuta el script 'eof_test.py', encargado de analizar los logs exportados y verificar que todos los nodos hayan recibido correctamente las señales de finalización (EOF).

De este modo, se puede corroborar la correcta implementación del mecanismo de finalización y la sincronización entre los distintos componentes del Sistema Distribuido.

## 📡 Monitorear RabbitMQ

Dado que el sistema utiliza RabbitMQ para la comunicación, podés seguir en tiempo real el estado de las colas, los mensajes que viajan y cómo se encadenan los procesos.

1. Primero, asegurate de haber levantado el sistema con make docker-compose-up.
2. Luego, entrá a la siguiente URL en tu navegador: [🔗 Link al gestor web](http://localhost:15672/)

### 📌 Credenciales por defecto:

- Usuario: guest
- Contraseña: guest

### ¿Qué nos permite hacer la interfaz del gestor?

Esta interfaz nos permite:

- Ver las colas activas.
- Inspeccionar mensajes.
- Observar cómo los controladores intercambian información.

## 🐵 Chaos-Monkey (Herramienta de creación de fallas)

Para generar fallos en el sistema distribuido (Ya sea de forma manual o automática) se desarrolló una herramienta de generacion de fallas del estilo 'Chaos-Monkey'.

Dentro del directorio 'chaos_monkey', se encuentra un 'README.md' con una explicación completa acerca de las configuraciones, uso y modos de funcionamiento de la misma.

## 📁 Archivos de entrada y salida

El sistema funciona con archivos de entrada y salida, se pasa a detallar el funcionamiento y ubicación de cada uno.

A continuación se pasa a detallar el funcionamiento al trabajar con el dataset completo.

### Archivos de entrada

Residen en el directorio ".data", estos son los que envía el cliente junto con las queries, y le brindan al sistema los datos para realizar el procesamiento pedido.

Por motivos de tamaño excesivo no se pueden cargas los datasets directamente en el repositorio, por lo que deben cargarse manualmente.

Para descargar el dataset completo se debe ingresar al siguiente link: [🔗 Link al dataset completo](https://www.kaggle.com/datasets/geraldooizx/g-coffee-shop-transaction-202307-to-202506/data)

También generamos nuestro propio dataset reducido (30%): [🔗 Link al dataset reducido](https://drive.google.com/drive/folders/1Zx6vl8iXw10OIUKS5Iz3qadV2ro_gW3f?usp=sharing)

### Archivos de salida

Las respuestas a las queries se generarán en archivos separados por cada una, que se crearán dentro del directorio '.results/query_results'.

Al finalizar la ejecución completa del procesamiento para todas las queries, dentro de ese directorio encontraremos el reporte final con los resultados para cada consulta realizada por el cliente.

## ♻️ Configuraciones del ambiente ('.env')

A fin de optimizar y modularizar el funcionamiento del sistema, se utiliza la herramienta del archivo '.env' para definir variables como:
- Rutas de donde los clientes obtienen los archivos de entrada.
- Cantidad de nodos instanciados por cada controlador.
- Tamaño de los 'Batchs' a enviar por cada mensaje.
- Credenciales de 'RabbitMQ'.
- LOGGING LEVEL.

Con esto se consiguió desacoplar el sistema lo máximo posible, y conseguir una óptima abstracción y separación de responsabilidades en la implementación.

Además, se elaboró un script que permite generar de forma automática el archivo 'docker-compose.yaml' en base a las variables de entorno definidas en el archivo.

Para ejecutar el mismo se debe utilizar el siguente comando:

```bash

./generar-compose.sh docker-compose.yaml

```

## 🎥 Desmotración de funcionamiento

[🔗 Link al video tutorial de funcionamiento](https://drive.google.com/drive/folders/1iDnXWh1Dd8fJBw4gxLcIzglYpP3rrnXQ?usp=sharing)
