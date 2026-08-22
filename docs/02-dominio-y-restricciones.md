# 02 — Dominio y restricciones

Este documento contiene **hechos**, no preferencias. Todo lo que sigue son
restricciones impuestas por la especificación del protocolo, por los navegadores,
por la infraestructura de red habitual o por el runtime de Go. El diseño debe
acomodarlas; no se pueden negociar.

El agente debe **verificar estos puntos contra la especificación vigente de WHATWG
HTML y la documentación de MDN** antes de diseñar, y ampliarlos si encuentra más.

---

## Glosario de trabajo

Vocabulario usado en el resto de documentos. No implica una estructura de tipos ni
de paquetes; el agente puede renombrar o reorganizar.

| Término | Significado |
|---|---|
| **Evento / mensaje** | Una unidad de información enviada al cliente, con su payload y sus metadatos. |
| **Stream** | La conexión HTTP de larga duración por la que viajan los eventos. |
| **Sesión** | El estado asociado a un stream concreto durante su vida. |
| **Suscriptor** | Un cliente conectado que recibe eventos. |
| **Topic** | La etiqueta o dirección lógica a la que se publica y por la que un cliente filtra. |
| **Historial** | El almacenamiento de eventos pasados que permite reanudar. |
| **Reanudación** | El proceso por el que un cliente que reconecta recupera lo que se perdió. |
| **Hueco (gap)** | La situación en la que un cliente reconecta pero el servidor no puede entregarle todo lo que perdió. |
| **Contrapresión** | El conjunto de comportamientos ante un consumidor que lee más lento de lo que se publica. |
| **Fusión / coalescing** | Descartar valores intermedios de una misma clave y quedarse solo con el último. |
| **Fan-out** | La entrega del mismo evento a muchos suscriptores. |

---

## El protocolo SSE

### Formato de cable

- El tipo de medio es `text/event-stream`.
- El flujo es texto y **debe codificarse en UTF-8**.
- Un evento es un bloque de texto terminado por un par de saltos de línea; los
  mensajes se separan entre sí por una línea en blanco.
- Los campos definidos son `data`, `event`, `id` y `retry`.
- Una línea que empieza por dos puntos es un **comentario** y el cliente la ignora.
- **Las líneas pueden separarse por CRLF, por LF solo, o por CR solo.** Las tres
  formas son válidas y un parser correcto debe aceptarlas todas. Es un error
  frecuente en implementaciones reales: hay librerías conocidas que solo parten por
  LF y por eso son incorrectas.
- Hay que hacer *stripping* de la marca de orden de bytes (BOM) si aparece.

### Semántica de los campos

- `data` es el único campo realmente necesario. **Un evento sin `data` se suprime**
  y el cliente no lo entrega.
- Si el payload abarca varias líneas, se repite el campo `data` en cada una; el
  cliente las concatena separadas por saltos de línea antes de exponerlas. Es el
  mismo mecanismo que usan las APIs de modelos de lenguaje para enviar texto
  parcial.
- Si no se envía `event`, el cliente dispara el evento genérico `message`. Si se
  envía, dispara un evento con ese nombre.
- `id` **no puede contener el carácter NUL**; si lo contiene, la línea se ignora.
- `retry` solo se aplica si su valor son dígitos ASCII; en caso contrario se
  ignora. Indica cuántos milisegundos debe esperar el cliente antes de reintentar.
  Si no se envía, el valor por defecto ronda los 3 segundos.

### El contrato de reanudación

- El cliente **guarda el último `id` que vio** y, al reconectar automáticamente, lo
  envía en la cabecera `Last-Event-ID`.
- El `id` es **opaco para el cliente**: lo devuelve tal cual, sin interpretarlo.
- Para que el servidor pueda reanudar, necesita que ese valor sea resoluble a una
  posición dentro de algún registro de eventos. La especificación no dice nada
  sobre cómo.
- **Problema no resuelto por la especificación, y que el diseño debe abordar
  explícitamente:** una conexión suscrita a varios topics tiene **un solo**
  `Last-Event-ID`, pero sus eventos provinieron de orígenes distintos. Un valor
  escalar no puede representar esa posición. Las implementaciones existentes
  revisadas fingen que existe un único stream ordenado, y por eso pierden eventos
  en silencio en cuanto hay más de un topic.
- **Segundo problema no resuelto:** si el almacenamiento de historial es volátil,
  reiniciar el proceso reinicia las posiciones. Un cliente que reconecte con un
  identificador anterior al reinicio podría recibir eventos distintos que
  casualmente ocupan esa posición. El fallo es silencioso y corrompe el estado del
  cliente. Debe existir alguna forma de detectar que un identificador pertenece a
  una generación anterior del historial.

### Método HTTP

SSE **no está limitado a GET**. Funciona con cualquier método, y protocolos como
MCP lo usan sobre POST. El diseño no debe asumir el método.

---

## Restricciones del navegador

- **La API `EventSource` no permite establecer cabeceras HTTP.** Esto convierte la
  autenticación en un problema real: las opciones son cookies, parámetros de query
  o un handshake previo. Existe además el patrón de usar `fetch` con streaming en
  lugar de `EventSource` para poder mandar cabeceras, a costa de reimplementar la
  reconexión.
- **`EventSource` no puede enviar datos al servidor.** Cualquier cambio de
  suscripción durante la vida del stream necesita un canal lateral (otra petición
  HTTP) o una reconexión.
- **Límite de conexiones en HTTP/1.1: seis por navegador y dominio.** Es un límite
  compartido entre todas las pestañas y ha sido marcado como "no se va a arreglar"
  en Chrome y en Firefox. Cada stream SSE consume una. Con HTTP/2 el número de
  streams simultáneos se negocia entre cliente y servidor, con un valor por defecto
  habitual de 100. Esto tiene una consecuencia directa de diseño: **conviene
  multiplexar varios flujos lógicos sobre una sola conexión en lugar de abrir
  varias**.
- El identificador de reanudación viaja como **cabecera HTTP**, así que debe ser
  ASCII válido para cabeceras y razonablemente corto: hay proxies que limitan el
  tamaño de cabeceras a entre 4 y 8 KB.
- El carácter `#` es un separador de fragmento en URLs: **el navegador trunca todo
  lo que va después**. Cualquier valor que viaje en un parámetro de query no puede
  usarlo.

---

## Restricciones de infraestructura

- **Proxies inversos que bufferean la respuesta** rompen el streaming. El caso
  conocido es nginx, que requiere una cabecera específica para desactivar el
  buffering de respuesta.
- **La compresión rompe el vaciado incremental** si no se maneja de forma
  consciente del flush.
- **Balanceadores de carga y proxies corporativos matan conexiones ociosas.** Un
  stream sin tráfico durante minutos puede ser cortado silenciosamente. La
  contramedida conocida son líneas de comentario periódicas que mantienen la
  conexión viva sin generar eventos en el cliente.
- Los comentarios sirven también como relleno frente a proxies y clientes antiguos
  que no entregan nada hasta acumular cierto volumen de bytes.
- En despliegues multinodo, si la reanudación depende de estado local del nodo, se
  necesitan sesiones pegajosas, con el coste operativo que eso implica.

---

## Restricciones del runtime de Go

- **`http.ResponseWriter` no es seguro para uso concurrente.** Escribir a una misma
  conexión desde más de una goroutine es incorrecto.
- El vaciado incremental requiere obtener la capacidad de flush del
  `ResponseWriter`. La comprobación clásica por aserción de tipo **falla cuando el
  writer viene envuelto por middleware**, incluso si el envoltorio expone el writer
  original. La biblioteca estándar ofrece desde Go 1.20 un mecanismo alternativo
  que sí atraviesa envoltorios y que además permite manipular *deadlines* de
  lectura y escritura por conexión.
- **La cancelación por contexto no es fiable en todos los entornos.** Caso
  documentado: sobre fasthttp (el motor de Fiber), la señal de finalización del
  contexto de petición **solo se dispara al apagar el servidor, no cuando el
  cliente se desconecta**, lo que deja suscriptores zombi que fugan memoria de
  forma permanente. Cualquier adaptador a un framework no basado en `net/http`
  debe resolver la detección de desconexión por su cuenta.
- **Una escritura contra un cliente que no lee puede bloquearse indefinidamente**
  si no hay un deadline de escritura. El contexto de petición no salva de esto.
- **Go 1.25 estabilizó `testing/synctest`**, que ejecuta pruebas en una burbuja con
  reloj virtual que solo avanza cuando todas las goroutines están bloqueadas de
  forma duradera. Es directamente relevante: convierte en deterministas y casi
  instantáneas las pruebas de heartbeats, deadlines, backoff y ventanas de
  retención, que de otro modo se escriben con esperas reales y resultan
  intermitentes en integración continua. **Verificar la versión mínima de Go y las
  garantías exactas antes de comprometerse.**
- El coste de una conexión SSE en Go se mide en goroutines, descriptores de fichero
  y memoria de buffers por conexión. A escala de decenas de miles de conexiones,
  cualquier estructura global protegida por un único mutex se convierte en el
  cuello de botella predecible.
