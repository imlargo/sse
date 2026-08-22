# 07 — Plan de trabajo

Ocho fases. Cada una **termina en algo verificable** y con una puerta de salida
trazada a requerimientos numerados de `03`. El orden no es negociable donde hay
dependencia real; donde no la hay, está marcado como paralelizable.

No hay estimaciones en semanas: dependen de tu disponibilidad. Hay **tamaño
relativo** (S / M / L) y **dependencias**, que es lo que sirve para planificar.

**Regla que atraviesa todas las fases:** ninguna fase se da por cerrada sin sus
pruebas. La suite corre siempre con `-race`, con detección de fugas de goroutines
en **todas** las pruebas (RP-4, RP-5) y con reloj virtual (`testing/synctest`)
para todo lo que dependa del tiempo (RP-3). Go mínimo: **1.25** (`synctest`
estable); desarrollo sobre 1.26.

---

## Fase 0 · Fundamentos — S

Sin código de producto. Es la infraestructura que hace que todo lo demás sea
verificable.

- Estructura de módulos de D-08. `go.mod` raíz sin dependencias.
- CI: `-race`, `go vet`, `staticcheck`, cobertura, y **la comprobación mecánica de
  RF-H1**: `go list -deps ./... | grep -v <stdlib>` debe salir vacío.
- Arnés de pruebas: helper de detección de fugas de goroutines aplicado por
  `TestMain` a toda la suite; envoltorio de `synctest`.
- `CONTRIBUTING.md`, política de versionado, plantilla de ADR.
- Congelar `06-decisiones-cerradas.md` como el ADR-0 del proyecto.

**Puerta de salida:** CI en verde sobre un repo vacío, y el chequeo de
dependencias fallando a propósito si se añade una dependencia externa al núcleo.

---

## Fase 1 · Capa de cable (`sse/wire`) — M · *paralelizable con Fase 2*

La pieza con valor autónomo. Se construye primero porque es la única con una
fuente normativa contra la que verificar, y porque todo lo demás la usa.

- Codificador: `data` multilínea, `event`, `id`, `retry`, comentarios,
  agrupamiento de varios eventos en una transmisión (RF-A10).
- Decodificador (para pruebas y para la suite de conformidad; **no** es el cliente
  Go, que está fuera de alcance de la v1).
- Validación en construcción con errores que enseñan (RF-G4): qué falló, por qué
  lo prohíbe la especificación, cuál es la forma correcta.
- Límite de tamaño de evento configurable (RF-G5).

**Casos que hay que acertar y que las librerías reales fallan** — cada uno es un
vector de la suite:

1. Los tres terminadores de línea: CRLF, LF solo, CR solo.
2. *Stripping* del BOM.
3. **Siempre emitir `data: ` con el espacio.** Solo se quita *un* espacio tras los
   dos puntos; emitirlo condicionalmente hace que todo payload que empiece por
   espacio pierda un carácter en silencio.
4. **`data:` con valor vacío SÍ despacha** un evento con `data == ""`. Solo se
   suprime el bloque *sin campo* `data`. Publicar un payload de longitud cero no
   es una operación nula.
5. **`id:` sin `data` avanza el cursor del cliente sin disparar evento.** Es el
   primitivo de checkpoint de D-04.
6. **`id:` con valor vacío resetea el cursor del cliente.** Es la señal de
   invalidación de D-04 / RF-C5.
7. NUL en `id` ⇒ campo ignorado por el cliente ⇒ el codificador debe rechazarlo
   en construcción.
8. `retry` no numérico ⇒ ignorado ⇒ rechazar en construcción.
9. Ningún valor de campo puede contener CR ni LF. **Es una invariante derivada del
   algoritmo de parseo, no una lista normativa** — `RF-A3` la atribuye a la
   especificación y la especificación no dice nada sobre caracteres del campo
   `event`. Se prueba lo derivado.
10. Campos desconocidos y comentarios ignorados; nombres de campo comparados
    literalmente, sin *case folding*.
11. El último bloque debe cerrarse con línea en blanco: **el EOF descarta un
    evento incompleto pendiente.**

**Puerta de salida (RF-A3, RF-G4, RF-G5, RP-1, RP-2):** suite de conformidad
publicada y reproducible con los once vectores anteriores como mínimo; fuzzing del
decodificador sin pánico ni asignación sin límite; benchmark de codificación con
presupuesto de asignaciones declarado.

---

## Fase 2 · Sesión y transporte — L · *paralelizable con Fase 1*

El nivel 0 completo. Es la fase donde se rompe con Medusa.

- **Modelo de una goroutine dueña del writer** (T-01): `Session.Send` entrega el
  frame; el writer es el único que toca el socket. RF-A2 imposible de violar por
  construcción.
- Cabeceras automáticas: `text/event-stream`, `Cache-Control: no-cache`,
  `Connection: keep-alive`, `X-Accel-Buffering: no` (RF-A6). Relleno inicial
  opcional **apagado por defecto** — es folklore de proxies antiguos, no está en
  la especificación, y `RF-A6` lo da por normativo.
- **Heartbeat = sonda de vida** (T-02): un solo temporizador, cadencia por defecto
  15 s (el consejo de la propia especificación WHATWG).
- Deadlines de escritura en toda escritura (RF-A8). Verificado: funcionan en
  HTTP/1.1 y en HTTP/2 con el http2 empaquetado de la stdlib.
- **Sondeo de capacidades al abrir** (RF-H4): `http.ResponseController` solo
  atraviesa envoltorios que implementan `Unwrap() http.ResponseWriter`. Si falta
  el flush, fallar en el acto con un error que **nombre el tipo concreto del
  writer**. Nunca abrir un stream que no se puede vaciar.
- Independencia del método (RF-A4), con el caso POST/MCP probado explícitamente
  — incluida la ruta con cuerpo sin drenar, donde `r.Context()` no se cancela.
- Apagado grácil (RF-E1): drenar, emitir `sse.closing`, cerrar el último
  bloque con línea en blanco.
- **Antiavalancha (RF-E2):** emitir `retry:` con jitter por conexión antes de
  cerrar. Es el único lever que el protocolo da y es suficiente.
- Espacio reservado `sse.*` configurable, con validación que impida al
  usuario invadirlo (RF-E4).
- Aislamiento de pánicos de código de usuario a la sesión afectada (RF-E5).
- Errores tipados (RNF-8).

**Trampa que hay que probar explícitamente:** un nodo drenando **jamás** puede
responder no-200 a una reconexión. Estado distinto de 200 o `Content-Type`
incorrecto ⇒ el cliente **falla permanentemente y no reintenta nunca más**. Es la
mina de RF-E1/E2, y también la herramienta correcta para RF-F3 (204 = "deja de
reconectar").

**Puerta de salida (RF-A1..A9, RF-E1..E5, RF-H4):** el ejemplo de nivel 0 escrito
y ejecutable; la prueba de RF-A7 —N conexiones cortadas en seco terminan con cero
goroutines y cero suscripciones residuales— en verde en los tres escenarios
(GET, POST sin drenar, y writer envuelto sin `Unwrap`).

---

## Fase 3 · El Log — L · *depende de 1 y 2*

El corazón. Fase 3 y 4 son el diferenciador; todo lo anterior es mesa puesta.

- Interfaz `Log` de cuatro operaciones (D-03).
- Implementación en memoria: ring de solo-anexado, frames inmutables, offsets
  monótonos, `epoch` por log, lectores sin tomar el lock de escritura.
- Retención conjunta por tiempo / eventos / bytes (RF-C9). Marca `Ephemeral`
  (D-06).
- **Códec del cursor** (D-04): `v1.<epoch>.<vector>`, base64url, presupuesto en
  bytes declarado. Si no cabe ⇒ la sesión declara `resumable: false` (RF-C12).
- Detección y declaración de huecos (RF-C4): offset pedido < offset más antiguo
  retenido ⇒ evento reservado `sse.gap` con el rango perdido, **entregado
  antes** de reanudar la entrega normal.
- Detección de generación anterior por `epoch` (RF-C5).
- Checkpoint del cursor en heartbeats mediante `id:` sin `data`.
- Evento `sse.open` con las capacidades (RF-E3): id de sesión, resumible
  sí/no, ventana de retención, filtros concedidos y denegados, semántica de
  entrega declarada.

**La propiedad que hay que probar y que ninguna otra librería tiene:** RF-C6, la
transición de historial a vivo sin duplicados, pérdidas ni desorden **aunque
lleguen eventos nuevos durante la reproducción**. En este modelo la carrera no
puede existir, porque reproducir *es* estar en vivo desde un offset más viejo. La
prueba debe demostrarlo bajo publicación concurrente sostenida.

**Puerta de salida (RF-C1..C9, RF-C12, RF-E3):** ejemplo de nivel 0 + historial
(progreso de trabajo largo con reanudación) ejecutable; prueba de RF-C6 con
publicación concurrente; prueba de que reiniciar el proceso invalida cursores
viejos en vez de resolverlos contra posiciones distintas.

---

## Fase 4 · Broker: topics, fan-out y contrapresión — L · *depende de 3*

- `Topic` y `Filter` tipados con validación y límites (RF-B6, RF-B7).
- Trie de tokens para coincidencia, O(tokens), sin regex ni asignaciones.
- Registro de suscriptores particionado — nada de un mapa global bajo un mutex
  único, que es el cuello de botella predecible que advierte `02` (RNF-2).
- Las cinco políticas de D-09, con `DropOldest` por defecto y presupuesto de
  retraso en **bytes**.
- Buffer de fusión por clave explícita (`Coalesce`), opt-in y con coste solo
  cuando se usa (RNF-4).
- Coherencia RF-D6: aviso por `slog` al construir y declaración en
  `sse.open` cuando se combina descarte agresivo con retención cero.
- Todo descarte y toda desconexión emiten señal observable con motivo
  distinguible (RF-D5).

**Puerta de salida (RF-B1..B7, RF-D1..D6, RNF-1..RNF-4):**

- Benchmark que demuestre que **el coste de serialización es independiente del
  número de suscriptores** (RNF-1).
- Benchmark de la ruta de publicación y entrega con **presupuesto de asignaciones
  declarado**, objetivo cero por evento entregado (RNF-3).
- Prueba de que un suscriptor deliberadamente lento **no afecta al throughput de
  los demás ni al del publicador** (RF-D3). Es la prueba que `tmaxmax/go-sse`
  suspende por diseño: su proveedor por defecto escribe síncronamente en el bucle
  de despacho.
- Ejemplo de dashboard de alta frecuencia (nivel 1) ejercitando la política de
  contrapresión.

---

## Fase 5 · Autorización y observabilidad — M · *depende de 4*

- `Authorizer` / `Grant` (D-07), con el `Cursor` ya decodificado y tipado
  entregado a la aplicación.
- Autorización por topic con denegación estructurada (RF-F2).
- `Grant.Deadline` para expiración de credenciales en vuelo (RF-F3).
- Logs por `log/slog`; interfaz `Metrics` propia, sin dependencias (RNF-9).
- Las ocho métricas de RNF-10, con la convención de nomenclatura de RNF-11:
  **sufijo `_node` en todo lo que sea local al nodo.**
- Módulos aparte: `metrics/prometheus`, `metrics/otel`.

**Puerta de salida (RF-F1..F4, RNF-8..RNF-11):** ejemplo de notificaciones
multi-tenant con autorización por topic (nivel 2); auditoría de que ninguna
métrica ni valor devuelto sugiere alcance global sin serlo.

---

## Fase 6 · Distribución — M · *depende de 5*

Aquí se cobra la apuesta de D-03. Si la costura está bien cortada, esta fase es
pequeña. **Si resulta grande, la costura estaba mal y hay que volver a la Fase 3**
— es el punto de control real del diseño.

- `logs/redislog` sobre Redis Streams (`XADD`/`XRANGE`/`XAUTOCLAIM`, ID `ms-seq`,
  `MAXLEN` para retención). Detección de hueco = comparar contra la primera
  entrada viva del stream.
- `logs/nats` sobre JetStream.
- `Presence` opcional para direccionar sesiones entre nodos (RF-B5, RF-F3).
- Pruebas de integración multinodo: **reconectar contra otro nodo y reanudar
  correctamente, sin sesiones pegajosas.**

**Puerta de salida (RF-H5, RF-C3, RF-C11, RNF-12):** el ejemplo de nivel 3 es el
de nivel 2 **con una línea cambiada**; prueba de reanudación cruzada entre nodos;
`logs/redislog` por debajo de ~250 líneas — si se pasa, la costura es demasiado
grande.

---

## Fase 7 · Catálogo y autodocumentación — M · *paralelizable con 6*

- `Declare[T](nombre)` — catálogo de eventos con comprobación en compilación
  (RF-G6).
- **Emisor de OpenAPI 3.2.** Esto ya no es una evaluación abierta: `04` dice que
  hay "una propuesta activa" y está desactualizado — **OpenAPI 3.2.0 salió con
  soporte SSE normativo**. `itemSchema` sobre `text/event-stream`, con el patrón
  documentado para streams heterogéneos: `oneOf` discriminado por
  `event: {const: "..."}`, más `contentMediaType: application/json` +
  `contentSchema` para el JSON dentro de `data`. **Es un mapeo 1:1 con el
  catálogo.** No se inventa ningún formato.
- Generación de tipos TypeScript y de un cliente tipado para el frontend, a partir
  del mismo catálogo. Es el diferenciador que ninguna librería Go de SSE ofrece.
- Derivar de la misma declaración el contenido de `sse.open` (RF-E3).

AsyncAPI queda como **segundo emisor opcional post-v1**, no como decisión
pendiente. Encaja además con que Medusa ya emita OpenAPI.

**Puerta de salida (RF-G6):** un catálogo declarado en Go produce un documento
OpenAPI 3.2 válido y tipos TS utilizables, sin duplicar la declaración.

---

## Fase 8 · Adaptadores, documentación y congelación — L · *depende de todo*

- `adapters/gin`, `adapters/echo`, `adapters/fibersse`. **Fiber es el importante**
  (RF-H3): es el hueco documentado del ecosistema y donde el diseño de T-02 se
  demuestra — sin depender de `ctx.Done()`, no hay suscriptores zombi.
- Los **cinco ejemplos** de `01`, ejecutables, en el repo. Son criterio de
  aceptación del diseño: si alguno sale feo o verboso, se corrige la librería, no
  el ejemplo.
- Tutorial progresivo y aditivo, con todos los ejemplos ejecutables (RP-8).
- README con los **no-objetivos declarados en voz alta** (RP-9) y con las
  garantías de la sección final de `06`.
- Prueba de carga sostenida: muchas conexiones, consumidores deliberadamente
  lentos, verificando que la memoria **se estabiliza** (RP-7).
- **Congelación de la API pública** y compromiso de compatibilidad (RNF-14).

**Puerta de salida:** la definición de "terminado" de `00`, entera.

---

## Fuera de alcance de la v1

Declarado aquí para que no se cuele por la puerta de atrás:

- Cliente Go (cerrado por el autor). El decodificador de `wire` **no** es un
  cliente: no tiene reconexión, ni backoff, ni gestión de cursor.
- Hub o binario autónomo (cerrado por el autor).
- Políticas de contrapresión compuestas o escalonadas.
- Motor de predicados para topics.
- Emisor de AsyncAPI.
- Adaptador de Kafka (la interfaz `Log` lo admite; no se construye en v1).

---

## Riesgos, y qué los mitiga

| Riesgo | Mitigación |
|---|---|
| **La costura de D-03 resulta demasiado grande** y `logs/redislog` acaba reimplementando lógica del núcleo | Es la puerta de salida explícita de la Fase 6. Si `logs/redislog` pasa de ~250 líneas, se vuelve a la Fase 3. Detectarlo tarde es el fracaso caro de este proyecto |
| **Dos goroutines por sesión** a 50 000 conexiones = 100 000 goroutines | Coste conocido y declarado en los benchmarks desde la Fase 2, no descubierto en la Fase 8. Es el precio de que RF-A2 y RF-A5 sean ciertos a la vez |
| El cursor vectorial **no cabe** en el presupuesto de cabecera con muchos logs | RF-C12 ya obliga a declarar `resumable: false` en vez de degradarse. La variante de handle en servidor existe como opción del 10% |
| Un log único es **un punto de contención** de escritura | Append de bytes ya codificados con lock de nanosegundos, lectores sin lock vía offset atómico. Particionar en varios logs está disponible cuando haga falta, con la pérdida de orden global documentada |
| `RF-C1` (historial apagado) frente a "el log existe siempre" | Resuelto en T-01 como promesa semántica. Hay que **probar** que con retención por defecto no se retiene nada consultable y `sse.open` declara `resumable: false` |
| La ergonomía se degrada al subir de nivel | El criterio cualitativo de `00` es la prueba: los cinco ejemplos se revisan al final de cada fase, no solo en la Fase 8 |

---

## Camino crítico

```
0 ──┬── 1 (wire) ──┐
    └── 2 (sesión) ─┴── 3 (log) ── 4 (broker) ── 5 (auth+obs) ── 6 (distribución) ──┐
                                                      └── 7 (catálogo+OpenAPI) ─────┴── 8
```

Fases 1 y 2 en paralelo. Fase 7 en paralelo con 6. Todo lo demás es secuencial
porque la dependencia es real, no organizativa.

**El punto de control que importa es el final de la Fase 6.** Hasta ahí, el
diseño es una hipótesis. Cuando `logs/redislog` salga corto y el ejemplo de nivel 3
sea el de nivel 2 con una línea cambiada, la hipótesis está confirmada.
