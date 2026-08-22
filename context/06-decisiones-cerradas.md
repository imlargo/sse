# 06 — Decisiones cerradas

Resuelve las diez bifurcaciones de `05-decisiones-abiertas.md` más dos decisiones
transversales que `05` no listaba y que resultaron ser las que mandan sobre el
resto. Cada decisión indica **qué se decide**, **por qué**, **contra qué prioridad
de `00`**, **qué requerimientos de `03` cubre** y **qué queda disponible para el
10% de casos que se salen del camino feliz**.

Nombre del proyecto: **`sse`** — ver D-10.

---

## Principio rector: 90 / 10

La regla que resuelve todos los empates que el orden de prioridades no resuelve
solo:

- **El 90% no decide nada.** El camino feliz tiene valores por defecto que
  funcionan: un solo log, cursor escalar, orden total, contrapresión razonable,
  heartbeat automático, historial apagado. El usuario no elige política, ni
  esquema de identificadores, ni partición.
- **El 10% obtiene flexibilidad por sustitución de piezas y por configuración,
  nunca por "escribe tú el bucle".**

De ahí sale la regla dura que gobierna la superficie pública:

> **No hay interfaces abiertas en la ruta de entrega.**
>
> **Enchufable:** el almacenamiento (`Log`), la serialización (`Codec`), las
> métricas (`Metrics`), el transporte (adaptadores) y la autorización
> (`Authorizer`).
>
> **Cerrado:** el formato de cable, el bucle de escritura, el conjunto de
> políticas de contrapresión y la asignación de identificadores de evento.

Lo cerrado es cerrado porque es donde vive la calidad de la librería. Una
interfaz abierta ahí es una invitación a que cada usuario reimplemente peor lo
que ya está bien hecho — que es exactamente el diagnóstico de `tmaxmax/go-sse`
en `04`.

---

## T-01 · Contrapresión e historial son un solo subsistema: el **Log**

**Decisión.** La unidad estructural del núcleo es un **log**: una secuencia
ordenada, de solo-anexado, direccionable por offset, de *frames ya codificados*.
Un suscriptor **no tiene una cola de eventos: tiene un offset sobre el log.**

Publicar es anexar al log y devolver. El publicador nunca toca a un suscriptor.
Entregar es leer desde mi offset hacia adelante, escribir al socket y avanzar.

**Por qué.** Descartar un evento por lentitud y perder un evento por desconexión
son el mismo suceso: el suscriptor se quedó atrás respecto de la cola del log.
Todas las librerías existentes los tratan como dos problemas y por eso el
descarte es pérdida de datos y la reanudación es una funcionalidad aparte y
frágil. Unificados, salen gratis siete cosas:

| Consecuencia | Requerimiento |
|---|---|
| Un lento no puede bloquear al publicador: el publicador nunca escribe a un suscriptor | RF-D3 |
| Una sola serialización para N suscriptores: los suscriptores guardan offsets, no payloads | RNF-1 |
| Coste por suscriptor O(1) en memoria, no O(profundidad de cola) | RF-D4, RNF-2 |
| Cero asignaciones por evento entregado: el frame es un `[]byte` inmutable compartido | RNF-3 |
| **La transición de historial a vivo no existe**: el offset simplemente avanza. No hay carrera posible por construcción | RF-C6 |
| "Descartar el más antiguo" pasa de ser pérdida **silenciosa** a ser pérdida **declarada**, con el rango exacto y un motivo distinguible | RF-D6 |
| Detección de hueco exacta y barata: offset pedido < offset más antiguo retenido | RF-C4 |

RF-C6 merece un subrayado: **toda librería SSE que reproduce historial y luego
"cambia a vivo" tiene una carrera en ese punto.** En un modelo de log la carrera
no puede existir, porque reproducir *es* estar en vivo, solo que desde un offset
más viejo.

**La tensión con RF-C1, y cómo se resuelve.** RF-C1 exige que el historial esté
desactivado por defecto. Aquí el log existe siempre, porque es el mecanismo de
fan-out. La resolución es que **RF-C1 es una promesa semántica, no una
estructural**: con retención por defecto (`Retention{}` = solo la ventana en
vuelo necesaria para el fan-out), el servidor **no ofrece reanudación**, el
evento de capacidades declara `resumable: false`, y un `Last-Event-ID` entrante
se responde con un hueco explícito. No se retiene nada consultable, no se promete
nada. Cuando el operador sube la retención, la reanudación se activa. Es la misma
estructura con otra ventana.

**Memoria acotada (RF-D4) en bytes, no en eventos.** Con 50 000 conexiones y una
cola de 64 eventos × 1 KB por suscriptor serían 3,2 GB. Aquí el presupuesto es
del **log** (bytes y/o eventos y/o tiempo, compartido), y el del suscriptor es un
**presupuesto de retraso** (`lag budget`) también en bytes. El coste por conexión
adicional es un offset y un buffer de escritura.

**Coste que sí hay que declarar.** Este modelo obliga a **dos goroutines por
sesión**: la que produce (el handler en nivel 0, el broker en niveles 1+) y **la
única dueña del writer**, que también emite heartbeats y avisos de drenaje. Es el
precio de que RF-A2 (imposible escribir desde dos sitios) y RF-A5 (heartbeat
garantizado aunque la aplicación no produzca nada) sean ciertos a la vez. Va
declarado en el presupuesto de los benchmarks, no escondido.

Consecuencia buena de ese mismo hecho: **el nivel 0 y el nivel 3 comparten la
misma maquinaria de sesión.** Los niveles son aditivos porque son literalmente el
mismo código con más piezas conectadas, no implementaciones paralelas.

---

## T-02 · El fallo de escritura es el mecanismo; el contexto es una optimización

**Decisión.** La detección de desconexión (RF-A7) se apoya en **una escritura
acotada por deadline que ocurre a una cadencia mínima garantizada**. El heartbeat
y la sonda de vida son **el mismo mecanismo**. `ctx.Done()` se observa como
atajo, pero **ninguna propiedad de corrección depende de él.**

**Por qué.** Verificado sobre tres entornos, la cancelación por contexto no es
fiable en ninguno de forma universal:

1. **`net/http` sobre HTTP/1.1 con cuerpo de petición sin drenar** — el caso MCP
   sobre POST (RF-A4). En `net/http/server.go`, la lectura en segundo plano que
   cancela `r.Context()` solo arranca si no queda cuerpo pendiente:
   `if requestBodyRemains(req.Body) { registerOnHitEOF(...) } else { startBackgroundRead() }`.
   Con un POST sin drenar, **`r.Context()` no se cancela nunca** al desconectarse
   el cliente.
2. **fasthttp / Fiber** — `RequestCtx.Done()` solo se dispara al apagar el
   servidor. Fuga permanente de suscriptores. Es el hueco documentado de RF-H3.
3. Fiber v3 ya resolvió lo suyo **por error de flush**, lo que corrobora de forma
   independiente que ese es el camino portable.

**Cómo se implementa.** El writer de la sesión mantiene un temporizador: si no ha
salido nada en `KeepAlive` (por defecto 15 s, que es el consejo de la propia
especificación WHATWG), emite un comentario `:` o una línea `id:` de checkpoint.
Toda escritura va envuelta en `SetWriteDeadline`. Un fallo o un vencimiento cierra
la sesión con un error tipado. Un cliente muerto se detecta, como mucho, en
`KeepAlive + WriteTimeout`, en cualquier entorno, con o sin contexto.

**Adaptadores.** Un adaptador solo tiene que proveer: escribir, vaciar, y
opcionalmente poner deadline. Si no hay deadline disponible (fasthttp), se degrada
a *detección por fallo de flush* y el adaptador lo declara; la sesión reporta la
capacidad reducida en vez de fingir. Nunca hay suscriptor zombi.

**`http.ResponseController` no es la respuesta completa (corrección a `02`).**
Solo atraviesa envoltorios **que implementan `Unwrap() http.ResponseWriter`** —
es un bucle de type-switch sobre `rwUnwrapper` en el fuente de la stdlib. Al
abrir el stream se hace un **sondeo de capacidades** (flush sí/no, deadline
sí/no); si falta el flush, el handler **falla en el acto con un error que nombra
el tipo concreto del writer y explica que debe implementar `Unwrap()`** (RF-G4,
RF-H4). Nunca se abre un stream que no se puede vaciar.

---

## D-01 · Modelo de coincidencia de topics → **tokens jerárquicos estilo NATS**

**Decisión.**

- Topic: tokens separados por `.`, cada token de `[A-Za-z0-9_-]+`.
- Filtro de suscripción: `*` = exactamente un token, `>` = uno o más tokens, **solo
  como último token**.
- **Comodines solo al suscribir.** Publicar siempre a un topic totalmente
  especificado.
- Límites (RF-B6): ≤ 16 tokens, ≤ 256 bytes, validados en el constructor.
- Tipos validados `Topic` y `Filter` con constructores `New…` (error) y `Must…`
  (pánico, para constantes), nunca `string` suelto por la API (RNF-6).
- Coincidencia por **trie de tokens**, O(tokens) sin regex y sin asignaciones.

**Por qué.** Bajo la prioridad #2 el criterio decisivo de `05` es la delegación
del filtrado a brokers externos, y es el único modelo del espacio que la cumple:
mapea directo a subjects de NATS, a topics de MQTT (`+`/`#`) y a patrones de
clave de Redis. Es también vocabulario que ya conoce cualquiera que venga de
mensajería.

**Descartado.** Las plantillas URI de Mercure son más expresivas pero **no se
pueden empujar al filtro de subject de ningún broker** — bajo la prioridad #2 eso
es descalificante. Los predicados arbitrarios además meten código de usuario en la
ruta caliente, con el riesgo de pánico y de coste impredecible.

**Nota RF-B7.** El charset elegido es seguro en query params sin escapado. `>`
se percent-codifica a `%3E`, que manejan todos los clientes. El `#` de MQTT
quedaría truncado por el navegador (`02` tiene razón) y por eso no se usa.

**RF-B4 no necesita maquinaria.** Multiplexar varios flujos lógicos sobre una
conexión **es una consecuencia** de tener filtros y una sesión, no una
funcionalidad aparte. No se construye nada para satisfacerlo.

**Para el 10%.** ¿Necesitas más expresividad? Suscríbete a un filtro más amplio y
filtra en la capa de aplicación con `Grant`, o usa más logs. No se añade un motor
de predicados.

---

## D-02 · Tipado del payload → **genéricos en el borde, bytes en el cable y en la costura**

**Decisión.** La tricotomía de `05` (bytes / `any` / genéricos) es falsa. Se
serializa **una sola vez, al publicar, antes del fan-out**; lo que viaja por
dentro y a través de la costura de escalado es un **frame inmutable ya
codificado**.

Superficie pública:

```go
b.Publish(ctx, topic, evento)                 // valor tipado, Codec por defecto (JSON)
b.Publish(ctx, topic, sse.Raw(bytes))    // bytes ya hechos
b.Publish(ctx, topic, sse.Text(s))       // texto crudo
b.Publish(ctx, topic, sse.From(r))       // desde un io.Reader
```

Y para quien quiere comprobación en compilación (RF-G6):

```go
var TicketCreated = sse.Declare[Ticket]("ticket.created")
TicketCreated.Publish(ctx, b, topic, t)       // no compila con otro tipo
```

**Por qué.** Lo que cruza una frontera de nodo son bytes, y RNF-1 prohíbe
serializar por suscriptor. Los genéricos viven solo en el borde, donde dan
ergonomía y seguridad de tipos, y **no contaminan** ni las interfaces de
transporte ni la de `Log` — que es lo que `05` temía.

**Sobre RNF-7.** El codec por defecto usa `encoding/json`, que reflexiona. **Eso
no es la ruta caliente:** ocurre **una vez por evento publicado**, no una vez por
suscriptor entregado. La ruta caliente —fan-out y escritura— es cero reflexión y
cero asignaciones. El `Codec` es sustituible (RF-G2) para quien quiera
codegen o una librería más rápida.

**RF-G3 en primera página.** `Raw`, `Text` y `From` no son escotillas avanzadas:
son el camino principal para htmx / Datastar / Turbo Streams y para proxies de
LLM. Van en el primer ejemplo de la documentación, no en un apéndice.

---

## D-03 · Costura de escalado → **el Log, y nada más**

**Decisión.** La pieza sustituible para pasar de un nodo a muchos es el `Log`.
Toda la interfaz:

```go
type Log interface {
    Append(ctx context.Context, f Frame) (Offset, error)
    Read(ctx context.Context, from Offset) (Reader, error)  // sigue la cola, bloqueante
    Info(ctx context.Context) (LogInfo, error)              // epoch, oldest, newest
}
```

Cuatro operaciones. **Redis Streams, NATS JetStream y Kafka las implementan
nativamente.** Una integración externa son ~150 líneas y hereda gratis: registro
de suscriptores, coincidencia de topics, colas por suscriptor, las cinco políticas
de contrapresión, reproducción, detección de huecos y métricas — porque **todo eso
vive por encima de la costura**, en código que el integrador no toca.

**Por qué, contra el criterio central de `05`.** La `Provider` de
`tmaxmax/go-sse` (`Publish`/`Subscribe`/`Shutdown`) es de grano grueso:
implementarla obliga a reimplementar el registro, el matching, las colas y la
contrapresión — todo aquello donde vive la calidad. Esta costura invierte eso: la
pieza sustituible es la más pequeña y la más aburrida del sistema, que es
exactamente lo que hace viable una contribución de la comunidad.

**Sesiones pegajosas: eliminadas.** Un cliente que reconecta contra el nodo B
presenta un cursor que nombra logs y offsets; B lee el mismo log compartido desde
ahí. **No hay estado de nodo involucrado en la reanudación.** Es la propiedad que
`05` marcaba como "merece perseguirse" y se consigue de forma completa.

**RNF-4.** En un nodo, el `Log` por defecto es un ring en memoria. Un handler de
nivel 0 **no instancia ningún log**: escribe directo. La abstracción de
distribución no cuesta nada cuando no se usa.

**Segunda costura, opcional (el 10%).** Para direccionar una sesión desde fuera
(RF-B5, RF-F3) hace falta un plano de control mínimo y separado —
`Presence`: registrar sesión, buscar, enviar señal. Es opt-in, no lo toca quien no
lo necesita, y no está en la ruta de datos.

---

## D-04 · Esquema del identificador de reanudación → **cursor opaco, versionado, vector sobre logs**

Es la decisión que manda sobre todas las demás, incluida D-03. `05` la ordenaba
al revés.

**Decisión.**

1. **El `id` del cable es el cursor.** La librería es dueña de él (ver D-05). No
   es un identificador de aplicación.
2. **El cursor es un vector sobre *logs*, no sobre topics.** Un topic pertenece a
   exactamente un log. Un log ingiere muchos topics. Es el modelo de NATS
   JetStream: filtrar por subject, reanudar por secuencia de stream.
3. **Por defecto hay un solo log ⇒ el cursor es un escalar de ~20 caracteres.**
4. Formato: `v1.<epoch>.<pares logID:offset, varint, delta, base64url>`. ASCII,
   seguro en cabecera y en URL, sin `#`.

**Por qué esto resuelve lo que `02` daba por no resuelto.** El problema declarado
—"una conexión con varios topics tiene un solo `Last-Event-ID`"— desaparece
porque **la unidad de posición deja de ser el topic**. Un suscriptor a `org.42.>`
puede recibir eventos de 500 topics concretos y su cursor sigue siendo un
escalar, porque todos esos topics viven en un log. Rastrear offsets por topic
concreto explotaría; rastrearlos por log no, porque el número de logs es una
constante que elige el operador, como las particiones de Kafka.

**Cobertura:**

| Requerimiento | Cómo |
|---|---|
| RF-C3 multi-topic real | El vector cubre todos los logs de la sesión; lo que no se pueda resolver se declara hueco por log |
| RF-C5 generación anterior | `epoch` por log, generado al crearlo. No coincide ⇒ irresoluble ⇒ hueco. Nunca se resuelve contra posiciones que ahora contienen otra cosa |
| RF-C12 presupuesto | Presupuesto declarado en bytes. Si el vector no cabe, la sesión **declara `resumable: false` al conectar**; nunca se degrada en silencio |
| Evolución | El prefijo `v1.` permite cambiar de esquema sin romper clientes con cursores viejos: un prefijo desconocido es un hueco declarado, no una corrupción |

**Dos primitivos de la especificación que hacen esto barato, y que ninguna
librería Go usa** (verificados contra WHATWG):

- En el algoritmo de despacho, el *last event ID buffer* se confirma **antes** del
  retorno temprano por data vacía. Luego **`id: <cursor>\n\n` —sin `data`— avanza
  el cursor del cliente sin disparar ningún evento.** Se usa para hacer
  *checkpoint* del cursor en los heartbeats, gratis.
- **`id:` con valor vacío resetea el cursor del cliente**, y entonces no se manda
  cabecera `Last-Event-ID` en la siguiente reconexión. Es la señal bendecida por
  el estándar para "tu cursor ya no vale, olvídalo" — exactamente lo que pide
  RF-C5, sin inventar nada.

**Coste en la ruta caliente.** Con **un log**, el `id` es idéntico para todos los
suscriptores ⇒ **el frame entero se comparte y se escribe de una sola vez, cero
asignaciones.** Con varios logs el `id` depende del suscriptor ⇒ se escribe
`[cuerpo compartido][línea id desde un scratch reutilizable]`, tres escrituras al
`bufio`, sigue siendo cero asignaciones. El camino feliz es también el óptimo.

**Para el 10%.** Si el vector no cabe en el presupuesto y hay un almacén
compartido disponible, existe la variante **cursor con handle en servidor**
(`id: <handle>:<seq>`, vector guardado en el mismo store que los logs). Es opt-in,
está documentada como lo que es, y no se activa sola.

---

## D-05 · Relación entre historial y difusión → **el log es el primitivo; la difusión es N lectores**

**Decisión.**

- **Difusión** = muchos suscriptores leyendo un log.
- **Un cliente con reanudación** = un suscriptor leyendo un log. **Misma
  maquinaria, N = 1.** RF-C8 queda satisfecho estructuralmente: el caso estrella
  de reanudación de LLM **no instancia nada de fan-out** porque no hay nada de
  fan-out que instanciar; el fan-out es una propiedad emergente del número de
  lectores.
- **Nivel 0 sin reanudación** no crea log: escribe directo (RF-B1).

**Quién asigna los identificadores: el log, al anexar.** No el publicador, no la
sesión. Es lo que hace que un `id` signifique una posición, que el esquema
funcione igual en un nodo y en varios, y que el frame se pueda compartir sin
copiar (RNF-1, RNF-3). El usuario puede llevar su propio identificador de
aplicación como un campo del payload; el `id` del cable es de la librería. Va
documentado en voz alta porque es una restricción con la que la gente se va a
topar.

---

## D-06 · Granularidad de la retención → **por log, y "más granularidad" es "más logs"**

**Decisión.** La retención se configura **por log**: por tiempo, por número de
eventos y por bytes, evaluados como límite conjunto. No hay retención por topic ni
por tipo de evento.

**Por qué.** La retención es una propiedad del almacenamiento, y el log *es* la
unidad de almacenamiento. Retención distinta por topic dentro de un log
compartido no es implementable sin sub-logs — que es, exactamente, usar más logs.
Así que RF-C10 `[DES]` se satisface **por composición** en lugar de por una
funcionalidad: ¿quieres retener las notificaciones 7 días y los ticks 30
segundos? Dos logs. Es honesto, cuesta cero y es como funcionan Redis y Kafka de
verdad.

**Una añadidura barata:** una marca por evento `sse.Ephemeral` = "entrégalo
en vivo, no lo retengas". Cubre el caso real de ticks de alta frecuencia dentro de
un log retenido, sin inventar una jerarquía de políticas.

---

## D-07 · Punto de decisión previo a la conexión → **una función, un valor**

**Decisión.** Nada de inyección de dependencias ni de contenedor. Un tipo función:

```go
type Authorizer func(*http.Request) (Grant, error)

type Grant struct {
    Identity  string      // opaco para la librería
    Filters   []Filter    // lo concedido; lo pedido y no concedido se deniega
    Log       LogRef      // a qué log se ata la sesión
    Policy    Policy      // política de contrapresión de esta sesión
    Deadline  time.Time   // fin forzado de sesión — RF-F3
}
```

Devolver un error con estado HTTP rechaza **antes** de abrir el stream (RF-F1).
Componible por composición de funciones normales. Sustituible en pruebas porque es
una `func`. Cero reflexión.

**La lección concreta de FastAPI**, que es la que importa: la sesión recibe el
`Last-Event-ID` **ya decodificado a un `Cursor` tipado**, no una cadena de
cabecera que el usuario va a excavar y parsear. El estado del protocolo se
declara, no se excava. En todas las librerías Go revisadas ese valor es trabajo
del usuario.

**RF-F2.** Un topic pedido y no concedido produce un rechazo 403 por defecto, o
—si se configura— un stream que arranca con un evento reservado
`sse.open` listando explícitamente `granted` y `denied`. Nunca un stream
vacío en silencio.

**RF-F3.** `Grant.Deadline` cierra la sesión al expirar el token. El cliente
reconecta solo por el mecanismo nativo del protocolo, con credenciales frescas, y
reanuda desde su cursor. Es el patrón de Mercure y convierte la expiración de
token en un no-evento.

---

## D-08 · Organización → **los paquetes ordenan conceptos, los módulos aíslan dependencias**

**Decisión.**

```
github.com/imlargo/sse                  módulo raíz — SOLO stdlib
  sse/                                  paquete principal
  sse/wire/                             formato de cable, valor autónomo
  sse/ssetest/                          transporte en memoria, helpers, detección de fugas

github.com/imlargo/sse/logs/redis       módulos aparte, go.mod propio
github.com/imlargo/sse/logs/nats
github.com/imlargo/sse/adapters/gin
github.com/imlargo/sse/adapters/fiber
github.com/imlargo/sse/adapters/echo
github.com/imlargo/sse/metrics/prometheus
github.com/imlargo/sse/metrics/otel
github.com/imlargo/sse/openapi
```

**RF-H1 se verifica mecánicamente en CI**, no por buena voluntad:
`go list -deps ./... | grep -v <stdlib>` debe salir vacío en el módulo raíz.

`wire` va aparte porque tiene valor por sí solo para quien únicamente quiere
producir o consumir el flujo — es el nicho que ocupa `jetify-com/sse` — y porque
permite probar la conformidad sin levantar un servidor.

---

## D-09 · Semántica de la contrapresión → **conjunto cerrado, `DropOldest` por defecto**

**Decisión.**

| Política | Semántica en el modelo de log |
|---|---|
| `DropOldest` **(defecto)** | El offset se queda atrás, la cola del log lo rebasa, se salta al nuevo tail y **se emite un hueco** |
| `DropNewest` | Se entregan los primeros `lag budget` pendientes, luego se salta al tail con hueco |
| `Coalesce` | Buffer por suscriptor con mapa ordenado `clave → pendiente`; un evento nuevo con clave ya pendiente supersede al viejo |
| `Block(timeout)` | Se espera al socket, acotado por el deadline de escritura. **Nunca bloquea al publicador**, solo a esta sesión |
| `Disconnect` | Retraso > presupuesto ⇒ cierre con error tipado `ErrSlowConsumer` |

- **Conjunto cerrado, no interfaz abierta.** Es lo que permite optimizar cada una,
  garantizar su comportamiento y no delegar lo difícil al usuario. Ampliable en
  v1.x sin romper nada.
- **Por defecto `DropOldest`.** No `Block` (bloquear al publicador es el bug de
  `r3labs/sse`), no `Disconnect` (demasiado agresivo por defecto), y **no
  elección obligatoria** — obligar a decidir en el camino feliz viola la
  prioridad #1. `DropOldest` degrada de forma predecible y **declara** lo que
  perdió.

  **Corrección tras implementarlo.** Un borrador anterior de este documento
  decía que en el modelo de log el descarte pasa de ser pérdida de datos a ser
  *recuperable*. Es una sobrepromesa y se detectó al escribir el código: los
  eventos descartados por contrapresión **están perdidos para esa conexión**.
  El cursor que el cliente guarda es el del último evento que recibió, así que
  al reconectar reanuda después del descarte. Lo que la librería sí hace —y es
  lo que ninguna otra hace— es decir **exactamente cuáles** se perdieron, con
  un motivo (`slow-consumer`) distinguible del vencimiento de retención
  (`retention`), y decirle al cliente que recargue estado. Mismo contrato que
  un hueco de retención. Un fallo declarado es aceptable; el silencioso no.
- **Clave de fusión explícita**, `sse.WithKey("entidad:42")`. Ni reflexión
  sobre el payload, ni derivada del topic. Cero magia.
- **Se fija al suscribir, inmutable durante la sesión** (RF-E6). Cambiarla es una
  sesión nueva; el canal lateral de RF-B5 puede renegociarla.
- **Sin políticas compuestas ni escalonadas en v1.** Combinatoria sin demanda
  demostrada.

**RF-D6, hecho coherente y no solo advertido.** Si la política descarta o
desconecta **y la retención del log es cero**, entonces: se emite un aviso por
`slog` al construir, y el evento `sse.open` declara al cliente
`delivery: at-most-once, recovery: none`. No se prohíbe la combinación —sería
paternalista— pero ni el operador ni el cliente pueden no enterarse.

---

## D-10 · Nombre → **`sse`**

**Decisión del autor.** Módulo `github.com/imlargo/sse`, paquete `sse`.

```go
import "github.com/imlargo/sse"

sse.Handler(...)   sse.New(...)   sse.MustTopic("org.42.tickets")
```

**Lo que gana:** el nombre dice exactamente qué es. No hay que explicarlo, no hay
metáfora que aprender, y el calificador de import es corto y se lee bien en cada
punto de llamada. Coincide además con el nombre del repositorio.

**El coste, declarado:** `sse` es también el nombre de paquete de
`tmaxmax/go-sse` y de `jetify-com/sse`. Quien use dos librerías de SSE a la vez
necesita un alias de import — molestia menor y poco frecuente. Y la
buscabilidad no viene del nombre, que es un término genérico y muy disputado,
sino del README, de los no-objetivos declarados en voz alta y de la suite de
conformidad publicada. Eso desplaza trabajo a la Fase 8, donde ya estaba.

**Espacio de eventos reservado** (RF-E4): prefijo `sse.` por defecto —
`sse.open`, `sse.gap`, `sse.closing` — configurable, con validación que impide
al usuario invadirlo.

---

## Validación cualitativa del diseño

`00` fija el criterio: si los cinco ejemplos se escriben cortos y legibles, el
diseño es bueno. Contraste:

**Nivel 0 — proxy de streaming de un LLM**

```go
http.Handle("/chat", sse.Handler(func(ctx context.Context, s *sse.Session) error {
    for tok := range llm.Stream(ctx, prompt) {
        if err := s.Send(ctx, sse.Text(tok)); err != nil {
            return err
        }
    }
    return nil
}))
```

Sin cabeceras, sin flush, sin heartbeat, sin deadline, sin bucle sobre un canal de
la librería. El bucle es sobre *su* fuente de datos, que es el shape de FastAPI.

**Nivel 2 — notificaciones multi-tenant**

```go
b := sse.New(sse.WithRetention(sse.Retention{For: 5 * time.Minute}))
http.Handle("/events", b.Handler(authorize))
b.Publish(ctx, sse.MustTopic("org.42.tickets"), ticket)
```

**Nivel 3 — lo mismo, en varios nodos**

```go
b := sse.New(
    sse.WithLog(redislog.New(client, "events")),   // <- única línea que cambia
    sse.WithRetention(sse.Retention{For: 5 * time.Minute}),
)
```

Una línea. Eso es la promesa de RF-H5 demostrada, no prometida.

---

## Garantías que la librería declara (RNF-12, RNF-13, RF-C2, RF-C7)

Se documentan así, en voz alta, y no se promete nada más:

- **Orden:** total dentro de un log. **Ninguna** entre logs distintos. Por
  defecto hay un log, así que por defecto hay orden total.
- **Entrega sin retención:** como mucho una vez, sin recuperación.
- **Entrega con retención:** al menos una vez dentro de la ventana. Nunca
  "exactamente una vez".
- **`Publish` devuelve cuando el evento está en el log**, no cuando se entregó. El
  nombre y la documentación lo dicen; no existe ninguna operación cuyo nombre
  sugiera confirmación de entrega.
- **Métricas locales al nodo llevan `_node` en el nombre** (RNF-11). Ninguna
  sugiere alcance global si no lo tiene.
