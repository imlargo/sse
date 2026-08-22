# 03 — Requerimientos

Cada requerimiento describe **qué debe ser cierto del producto terminado**, no cómo
lograrlo. Donde aparece un criterio de aceptación, es la forma de comprobarlo.

**Prioridad:** `[OBL]` obligatorio para la v1 · `[DES]` deseable en v1 ·
`[POST]` posterior a la v1.

---

## A. Núcleo del stream

### RF-A1 `[OBL]` La librería es dueña de sus bucles
El usuario nunca escribe un bucle de lectura ni de escritura, ni establece
cabeceras HTTP, ni invoca vaciados de buffer manualmente.

*Criterio:* ningún ejemplo del repositorio contiene un bucle sobre un canal de
eventos ni una llamada explícita a flush.

*Justificación:* es el punto de ruptura con la implementación actual de Medusa. Si
el bucle es del usuario, la librería no puede ser responsable de heartbeats,
deadlines, orden de reproducción ni drenaje, y ninguna mejora futura puede
aplicarse sin romper a todos los usuarios.

### RF-A2 `[OBL]` Escritura correcta y segura a la conexión
Debe ser imposible por construcción que dos flujos de ejecución escriban
simultáneamente a la misma conexión.

### RF-A3 `[OBL]` Conformidad con el formato de cable
La producción de eventos cumple la especificación en todos sus detalles: campos
válidos, separación de mensajes, división de payloads multilínea, comentarios,
codificación UTF-8, restricciones sobre caracteres prohibidos en `id` y en el
nombre del evento.

*Criterio:* existe una suite de conformidad con vectores derivados de la
especificación, y pasa.

### RF-A4 `[OBL]` Independencia del método HTTP
Funciona sobre GET, POST y cualquier otro método. No se asume GET en ninguna parte
de la API ni de la documentación.

### RF-A5 `[OBL]` Mantenimiento de la conexión viva
Emisión periódica y configurable de tráfico que impida que proxies y balanceadores
corten conexiones ociosas, sin que ese tráfico produzca eventos visibles en el
cliente.

### RF-A6 `[OBL]` Neutralización del buffering intermedio
Las respuestas se emiten de forma que los proxies inversos habituales no las
retengan. Debe cubrirse tanto el caso de nginx como el de clientes o proxies que
requieren un volumen mínimo de bytes antes de entregar nada.

### RF-A7 `[OBL]` Detección fiable de desconexión
Un cliente que se desconecta libera todos sus recursos, en todos los entornos
soportados, incluidos aquellos donde la cancelación por contexto no es fiable.

*Criterio:* una prueba que abre N conexiones y las corta abruptamente termina con
cero goroutines y cero suscripciones residuales.

### RF-A8 `[OBL]` Un cliente que no lee no bloquea recursos indefinidamente
Existe un límite temporal aplicado a las escrituras, configurable, tras el cual la
conexión se considera fallida y se libera.

### RF-A9 `[OBL]` Errores de escritura observables
Cuando una escritura falla, el hecho es visible para la aplicación con información
suficiente para distinguir la causa. Nada de fallos silenciosos.

### RF-A10 `[DES]` Agrupamiento de eventos
Posibilidad de enviar varios eventos en una sola transmisión cuando conviene por
rendimiento.

---

## B. Difusión, topics y suscripción

### RF-B1 `[OBL]` El caso de un solo cliente no requiere maquinaria de difusión
Un stream de un solo cliente sin difusión debe poder construirse sin instanciar
registro de suscriptores, sin topics y sin goroutines de fan-out. Si no se pide,
no existe y no cuesta.

*Justificación:* es el nivel 0 de `01-casos-de-uso.md` y probablemente la mayoría
de los usos.

### RF-B2 `[OBL]` Difusión a múltiples suscriptores
Publicar un evento y que lo reciban todos los suscriptores conectados.

### RF-B3 `[OBL]` Segmentación por topic
Los suscriptores declaran qué quieren recibir y solo reciben eso. El modelo de
coincidencia concreto es una decisión abierta (ver `05`), pero debe cubrir al menos
el caso multi-tenant: segmentación por organización, proyecto o entidad, con
suscripciones que abarquen conjuntos de topics y no solo topics individuales.

### RF-B4 `[OBL]` Multiplexación sobre una sola conexión
Un cliente debe poder recibir varios flujos lógicos distintos sobre una única
conexión HTTP. Es consecuencia directa del límite de seis conexiones por dominio
en HTTP/1.1.

### RF-B5 `[DES]` Cambio de suscripción sin reconectar
Un cliente debe poder modificar a qué está suscrito durante la vida del stream. La
API `EventSource` no puede enviar datos, así que esto requiere algún canal lateral,
lo cual implica que la sesión debe ser direccionable desde fuera.

### RF-B6 `[OBL]` Validación de topics con límites
Los identificadores de topic se validan al construirse, con límites de tamaño y de
estructura que impidan que un valor arbitrario del cliente degrade el servidor.

### RF-B7 `[OBL]` Compatibilidad con transporte en parámetros de query
Los identificadores de topic y de suscripción deben poder viajar en una URL sin
sufrir truncamiento ni requerir escapado especial, porque `EventSource` no puede
enviar cabeceras.

---

## C. Historial y reanudación

### RF-C1 `[OBL]` El historial está desactivado por defecto
Un servidor mínimo no retiene nada y no promete nada. Activar el historial es una
acción explícita.

*Justificación:* no prometer es mejor que prometer a medias.

### RF-C2 `[OBL]` Semántica de entrega declarada explícitamente
La aplicación sabe, y puede consultar, qué garantía tiene: sin historial, entrega
como mucho una vez y sin recuperación; con historial, entrega al menos una vez
dentro de una ventana de retención definida. Nunca se documenta ni se insinúa una
garantía más fuerte de la que se cumple.

### RF-C3 `[OBL]` Reanudación funcional con múltiples topics
Un cliente suscrito a varios topics que reconecta debe recuperar lo perdido en
**todos** ellos, o ser informado de que no se pudo. El diseño debe resolver
explícitamente el problema de que la especificación solo provee un valor escalar de
reanudación (ver `02-dominio-y-restricciones.md`).

### RF-C4 `[OBL]` Declaración explícita de huecos
Cuando el servidor no puede entregar todo lo que el cliente perdió, se lo dice.
Debe haber una señal explícita, distinguible de los eventos de la aplicación, que
identifique qué parte del flujo quedó incompleta, entregada **antes** de reanudar
la entrega normal.

*Este es el requerimiento central del documento.* La regla que lo resume:
**la librería nunca finge**. Un fallo declarado es aceptable; un fallo silencioso
que corrompe el estado del cliente no lo es.

### RF-C5 `[OBL]` Detección de identificadores de una generación anterior
Si el historial perdió su contenido —por reinicio del proceso, por cambio de
almacenamiento o por cualquier otra causa— un identificador de reanudación anterior
debe detectarse como no resoluble y tratarse como hueco, nunca resolverse contra
posiciones que ahora contienen eventos distintos.

### RF-C6 `[OBL]` Transición sin duplicados ni desorden
El paso de la reproducción del historial a la entrega en vivo no puede producir
eventos duplicados, perdidos ni fuera de orden, aunque lleguen eventos nuevos
mientras se está reproduciendo.

### RF-C7 `[OBL]` Orden de reproducción declarado
El orden en el que se reproducen eventos de varios topics debe estar documentado
como garantía. No se debe ofrecer una reconstrucción de orden global si no se puede
sostener.

### RF-C8 `[OBL]` El historial es usable sin difusión
El caso de un solo cliente con reanudación (nivel 0 con infraestructura de nivel 3)
no debe exigir instanciar la maquinaria de difusión y topics.

*Justificación:* es exactamente la forma del caso de reanudación de streams de LLM,
que es el caso de mayor demanda actual.

### RF-C9 `[OBL]` Retención configurable
La política de cuánto se retiene debe ser configurable, al menos por tiempo y por
número de eventos.

### RF-C10 `[DES]` Granularidad de retención
Poder retener de forma distinta según el tipo de evento o el topic, y no de forma
uniforme para todo.

### RF-C11 `[OBL]` Almacenamiento de historial sustituible
Debe ser posible sustituir el almacenamiento en memoria por uno externo o
persistente sin que cambie el código de la aplicación.

### RF-C12 `[OBL]` Presupuesto del identificador de reanudación
El valor que el cliente devuelve viaja en una cabecera HTTP y debe respetar los
límites de tamaño de cabecera de la infraestructura habitual. Si por la
configuración de una sesión ese presupuesto no se puede respetar, la sesión debe
**declarar que no soporta reanudación**, nunca degradarse en silencio.

---

## D. Contrapresión y control de flujo

### RF-D1 `[OBL]` La política es elegible, no impuesta
El comportamiento ante un consumidor lento es una decisión de la aplicación,
configurable con granularidad de suscripción. Ni bloquear al publicador ni
descartar en silencio pueden ser el único comportamiento disponible.

### RF-D2 `[OBL]` Conjunto mínimo de políticas
Debe cubrirse al menos: esperar con límite temporal, descartar los eventos más
antiguos, descartar los más recientes, **fusionar por clave** conservando solo el
último valor de cada entidad, y desconectar al consumidor lento.

La fusión por clave es la que más falta hace en casos de sincronización de estado y
dashboards, y la que menos librerías ofrecen.

### RF-D3 `[OBL]` Un consumidor lento no degrada a los demás
Publicar nunca puede quedar bloqueado por el ritmo de lectura de un suscriptor
individual, salvo que la política elegida lo pida explícitamente.

### RF-D4 `[OBL]` Memoria acotada por suscripción
Existe un límite superior conocido y configurable de memoria retenida por
suscriptor.

### RF-D5 `[OBL]` Descartes y desconexiones observables
Todo evento descartado y toda desconexión por lentitud produce una señal
observable, con motivo distinguible.

### RF-D6 `[OBL]` Coherencia entre contrapresión y reanudación
Desconectar a un consumidor lento solo es una política aceptable si existe un
contrato de reanudación que permita recuperar. La librería debe hacer coherente esa
relación: no ofrecer descarte agresivo como opción sin señalar sus consecuencias
cuando el historial está desactivado.

---

## E. Ciclo de vida

### RF-E1 `[OBL]` Apagado grácil
Al apagar, las conexiones abiertas se drenan de forma ordenada dentro de un plazo
configurable, y los clientes son informados antes del cierre.

### RF-E2 `[OBL]` Prevención de avalancha de reconexión
Si decenas de miles de clientes se desconectan a la vez, no deben reconectar
simultáneamente. La librería debe influir en el momento de reconexión de cada
cliente para dispersar la carga.

### RF-E3 `[OBL]` Señalización al cliente al conectar
Al establecerse el stream, el cliente recibe información sobre qué se le está
ofreciendo: identificador de sesión, si hay reanudación disponible, cuál es la
ventana de retención, y qué parte de lo que pidió quedó aceptada.

*Justificación:* el cliente debe saber qué se le prometió. Es autodocumentación en
tiempo de ejecución.

### RF-E4 `[OBL]` Espacio de nombres reservado
Los eventos que genera la propia librería (conexión establecida, hueco detectado,
aviso de apagado) no pueden colisionar con los de la aplicación. Debe existir un
prefijo o espacio reservado, configurable, con validación que impida al usuario
invadirlo.

### RF-E5 `[OBL]` Aislamiento de fallos
Si código proporcionado por el usuario entra en pánico, el daño se contiene en la
suscripción o petición afectada y no derriba el proceso ni el registro de
suscriptores.

### RF-E6 `[OBL]` Configuración inmutable tras el arranque
No debe existir estado público mutable que pueda cambiarse después de que el
servidor esté sirviendo. Es una fuente conocida de condiciones de carrera en
librerías existentes.

---

## F. Autenticación y autorización

### RF-F1 `[OBL]` Punto de decisión antes de abrir el stream
Debe existir un punto claro donde la aplicación, con acceso a la petición HTTP
completa, decide si acepta la conexión, qué identidad tiene, a qué topics puede
suscribirse y con qué política. Un rechazo ahí produce una respuesta HTTP normal
con su código de estado, **antes** de que el stream se abra.

### RF-F2 `[OBL]` Autorización por topic
La aplicación puede autorizar o denegar topics individualmente. Un topic denegado
produce un error estructurado, nunca un stream vacío en silencio.

### RF-F3 `[DES]` Manejo de expiración de credenciales en vuelo
Los tokens caducan mientras el stream sigue abierto. Debe haber una forma de que
la aplicación fuerce el fin de la sesión para que el cliente reconecte con
credenciales frescas, aprovechando la reconexión automática del protocolo.

### RF-F4 `[OBL]` Compatibilidad con autenticación sin cabeceras
El diseño no puede asumir que el cliente puede enviar cabeceras de autorización,
porque `EventSource` no puede.

---

## G. Payload, tipos y autodocumentación

### RF-G1 `[OBL]` Serialización ergonómica por defecto
Publicar un valor de la aplicación no debe obligar a serializarlo y manejar el
error en cada punto de llamada.

### RF-G2 `[OBL]` Serialización sustituible
El mecanismo de serialización debe poder cambiarse, para permitir alternativas de
mayor rendimiento u otros formatos.

### RF-G3 `[OBL]` Escotillas para contenido ya serializado
Debe ser trivial publicar bytes, texto o contenido leído de un flujo, sin pasar por
serialización. **No es un caso avanzado:** es el camino principal para interfaces
hipermedia (htmx, Datastar, Turbo Streams), que empujan fragmentos de HTML, y para
proxies de modelos de lenguaje, que ya reciben los bytes hechos. Debe estar en la
primera página de la documentación, no escondido.

### RF-G4 `[OBL]` Validación al construir, con errores que enseñan
Las invariantes del protocolo se comprueban en el momento de construir el evento,
no al escribirlo en el cable. El error dice qué falló, por qué la especificación lo
prohíbe y cuál es la forma correcta.

### RF-G5 `[OBL]` Límites de tamaño
Existe un límite configurable al tamaño de un evento. Un evento
desproporcionadamente grande multiplicado por miles de suscriptores es una
denegación de servicio contra uno mismo.

### RF-G6 `[DES]` Catálogo de eventos declarado
La aplicación puede declarar, en un solo sitio, el conjunto de tipos de evento que
su stream emite y la forma de cada payload. De esa declaración deberían poder
derivarse, sin duplicar información:

- La información de capacidades que se envía al cliente al conectar (RF-E3).
- Comprobación en tiempo de compilación de que no se publica un tipo no declarado.
- **Generación de tipos y de un cliente tipado para el frontend.** Declarar los
  eventos una vez en Go y que el frontend tenga tipos es un diferenciador que
  ninguna librería Go de SSE ofrece hoy.
- Un documento de descripción de la API en un formato estándar.

*Nota:* existe precedente parcial en Go (ver `04-investigacion-previa.md`), atado a
un framework concreto. Aquí debe ser agnóstico. **No inventar un formato de
descripción propio**: OpenAPI tiene entrada de registro para `text/event-stream` con
consideraciones específicas y hay una propuesta activa para soporte nativo;
AsyncAPI también se usa para describir APIs SSE reales. El agente debe evaluar
ambos.

---

## H. Integración y dependencias

### RF-H1 `[OBL]` Núcleo sin dependencias externas
Usar la librería con la biblioteca estándar no arrastra Gin, Fiber, Echo, Redis,
NATS, Prometheus ni OpenTelemetry.

*Criterio:* el grafo de dependencias del núcleo contiene únicamente la biblioteca
estándar.

### RF-H2 `[OBL]` Agnóstico de framework
Se integra con `net/http` directamente y con los frameworks populares mediante
adaptadores que no contaminan el núcleo.

### RF-H3 `[OBL]` Soporte de frameworks no basados en `net/http`
Debe contemplarse explícitamente el caso de motores como fasthttp/Fiber, donde la
detección de desconexión de cliente no funciona por los medios habituales y las
librerías existentes fugan suscriptores de forma permanente. Es un hueco real y
documentado del ecosistema.

### RF-H4 `[OBL]` Compatible con middleware que envuelve el `ResponseWriter`
La librería debe seguir funcionando cuando el writer viene envuelto por middleware
de terceros (logging, compresión, métricas).

### RF-H5 `[OBL]` Piezas de escalado sustituibles sin reescribir la aplicación
Pasar de un nodo a varios debe ser cuestión de sustituir piezas, no de cambiar el
código de la aplicación. Debe evaluarse cuidadosamente **qué tamaño tiene la pieza
sustituible** (decisión abierta, ver `05`): si es demasiado grande, cada
integración con un sistema externo acaba reimplementando la lógica donde vive la
calidad de la librería, y cada una lo hará distinto y peor.

---

## I. Requerimientos no funcionales

### RNF-1 `[OBL]` Coste de fan-out
Entregar el mismo evento a N suscriptores no debe requerir N serializaciones del
mismo contenido. El coste por suscriptor adicional debe acercarse al de una
escritura.

*Criterio:* un benchmark de difusión demuestra que el coste de serialización es
independiente del número de suscriptores.

### RNF-2 `[OBL]` Escalabilidad del registro de suscriptores
El rendimiento no debe degradarse de forma no lineal al crecer el número de
conexiones. Objetivo de orden de magnitud: decenas de miles de conexiones
concurrentes por proceso.

### RNF-3 `[OBL]` Presupuesto de asignaciones en la ruta caliente
La ruta de publicación y entrega tiene un presupuesto de asignaciones declarado y
verificado por benchmark, con el objetivo de acercarse a cero asignaciones por
evento entregado.

### RNF-4 `[OBL]` Coste cero de lo no usado
Las capacidades no solicitadas (difusión, topics, historial, distribución,
métricas) no deben costar memoria, goroutines ni trabajo en tiempo de ejecución.

### RNF-5 `[OBL]` Superficie conceptual mínima
El caso común debe requerir aprender un número muy pequeño de conceptos. Todo lo
demás debe estar disponible pero no presente en el camino simple.

*Criterio cualitativo:* el ejemplo mínimo cabe en pocas líneas y no menciona
ningún concepto avanzado.

### RNF-6 `[OBL]` La API se descubre desde el editor
Tipado suficiente para que el autocompletado y la documentación en línea guíen al
usuario sin abrir el README. Se prefieren tipos validados a cadenas de texto
sueltas circulando por la API.

### RNF-7 `[OBL]` Sin magia en tiempo de ejecución
No se debe usar reflexión ni introspección en la ruta caliente. La sensación
declarativa debe conseguirse con tipos y, si hace falta, generación de código.

### RNF-8 `[OBL]` Errores como valores
Errores tipados, distinguibles programáticamente por causa: consumidor lento, hueco
de historial, sesión cerrada, capacidad no soportada, topic no autorizado. Nada de
pánicos que crucen la frontera pública.

### RNF-9 `[OBL]` Observabilidad sin dependencias impuestas
Logs mediante la interfaz de la biblioteca estándar. Métricas mediante una interfaz
propia, con integraciones concretas fuera del núcleo.

### RNF-10 `[OBL]` Métricas mínimas
Conexiones activas, eventos publicados, eventos entregados, eventos descartados por
motivo, desconexiones por motivo, latencia de publicación a entrega, ocupación de
colas, tamaño del historial.

### RNF-11 `[OBL]` Honestidad de las métricas en despliegues distribuidos
Ninguna métrica ni valor devuelto debe sugerir un alcance global si su valor es
local al nodo. La nomenclatura debe reflejarlo.

### RNF-12 `[OBL]` Garantías de orden declaradas y sostenibles
El orden que se promete debe ser el que realmente se puede sostener con la
infraestructura subyacente, y debe estar documentado. No se debe prometer orden
total entre nodos si no se puede garantizar.

### RNF-13 `[OBL]` Semántica de la publicación honesta
El resultado de publicar debe reflejar exactamente qué se garantizó: aceptación
local no es lo mismo que entrega. La API no debe ofrecer operaciones cuyo nombre
sugiera una confirmación de entrega que no puede darse.

### RNF-14 `[OBL]` Estabilidad de la API pública
Compromiso de compatibilidad hacia atrás a partir de la v1. Lo que sea susceptible
de cambiar debe quedar fuera de la superficie estable.

---

## J. Calidad del proyecto

### RP-1 `[OBL]` Suite de conformidad con la especificación
Vectores de prueba derivados de la especificación, cubriendo los casos límite:
marca de orden de bytes, los tres terminadores de línea, caracteres prohibidos en
`id`, valores no numéricos en `retry`, eventos sin payload, campos desconocidos y
comentarios. Publicada y reproducible: es lo que distingue a una librería seria de
"otra librería de SSE".

### RP-2 `[OBL]` Fuzzing del parser
Entrada arbitraria: nunca debe provocar pánico ni asignación sin límite.

### RP-3 `[OBL]` Pruebas de concurrencia deterministas
Las pruebas de comportamiento dependiente del tiempo (heartbeats, deadlines,
backoff, ventanas de retención) no deben depender de esperas reales. Evaluar el
soporte de reloj virtual de la biblioteca estándar (ver
`02-dominio-y-restricciones.md`).

### RP-4 `[OBL]` Detección de fugas de goroutines
Verificada en cada prueba, no solo en algunas.

### RP-5 `[OBL]` Detector de carreras en integración continua
Toda la suite corre con detección de carreras activada.

### RP-6 `[OBL]` Benchmarks reproducibles
Con presupuesto de asignaciones declarado y comparables entre versiones.

### RP-7 `[DES]` Prueba de carga sostenida
Con muchas conexiones y consumidores deliberadamente lentos, verificando que la
memoria se estabiliza.

### RP-8 `[OBL]` La documentación es parte del producto
Tutorial progresivo donde cada paso es aditivo, todos los ejemplos ejecutables, y
los cinco ejemplos de referencia de `01-casos-de-uso.md` presentes en el
repositorio. Este requisito es de la v1, no de "algún día".

### RP-9 `[OBL]` Límites declarados en voz alta
El README declara explícitamente los no-objetivos de `01-casos-de-uso.md`.
