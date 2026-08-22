# Librería SSE en Go — Paquete de contexto y requerimientos

Conjunto de documentos que define **qué** hay que construir. No define **cómo**.

Estos archivos son el punto de partida para un agente o equipo que debe investigar
el espacio de soluciones, evaluar alternativas y tomar las decisiones técnicas de
arquitectura, diseño e implementación por sí mismo.

## Orden de lectura

| Archivo | Contenido |
|---|---|
| `00-contexto.md` | Propósito, trasfondo, perfil del autor, relación con el framework Medusa, principios rectores y prioridades ordenadas. **Empezar aquí.** |
| `01-casos-de-uso.md` | Casos de uso reales que la librería debe cubrir, agrupados por nivel de complejidad, y casos que explícitamente quedan fuera. |
| `02-dominio-y-restricciones.md` | Glosario, funcionamiento del protocolo SSE y restricciones inmutables que impone la especificación, los navegadores, la infraestructura y el runtime de Go. Hechos, no opiniones. |
| `03-requerimientos.md` | Requerimientos funcionales y no funcionales numerados, con criterios de aceptación. El núcleo del encargo. |
| `04-investigacion-previa.md` | Estado del arte: qué librerías existen en Go y en otros lenguajes, qué resuelve cada una y dónde falla. Referencias verificadas. |
| `05-decisiones-abiertas.md` | Las bifurcaciones de diseño identificadas, con el espacio de opciones y los criterios para elegir. Planteamiento original; **resueltas en `06`.** |
| `06-decisiones-cerradas.md` | Las diez decisiones de `05` resueltas y justificadas, más dos transversales que mandan sobre el resto. Es el ADR-0 del proyecto. |
| `07-plan-de-trabajo.md` | Ocho fases con dependencias, tamaño relativo y puertas de salida trazadas a requerimientos. |

## Cómo usar este paquete

1. Leer `00` y `03` completos antes de proponer nada.
2. Tratar `02` como restricciones duras: son hechos del protocolo y del entorno,
   no preferencias negociables.
3. Tratar `05` como el trabajo real de diseño. Cada decisión listada ahí debe
   resolverse con investigación propia y justificarse contra los criterios dados
   y contra las prioridades de `00`.
4. Cualquier propuesta de arquitectura debe poder trazarse de vuelta a un
   requerimiento numerado de `03` o a un caso de uso de `01`. Si no se puede,
   probablemente sobra.

## Estado

Arquitectura cerrada (`06`) y fases 0 a 2 del plan (`07`) implementadas.

| Fase | Estado |
|---|---|
| 0 · Fundamentos | hecha — módulo sin dependencias verificado en CI, detección de fugas en toda la suite |
| 1 · Capa de cable (`wire/`) | hecha — 32 vectores de conformidad, fuzzing, **0 asignaciones por evento** |
| 2 · Sesión y transporte | hecha — nivel 0 completo, con ejemplo ejecutable |
| 3 · El Log | hecha — offsets, epoch, retención, cursor, huecos declarados |
| 4 · Broker: topics y contrapresión | hecha — filtros jerárquicos, cinco políticas, checkpoint de cursor |
| 5 · Autorización y observabilidad | hecha — Authorizer/Grant, denegación declarada, métricas sin dependencias |
| 6 · Distribución | hecha — Redis Streams, reanudación entre nodos verificada |
| 7 · Catálogo y OpenAPI 3.2 | hecha — sin tipos TS (descartado por el autor) |
| 8 · Adaptadores, docs y congelación | parcial, ver abajo |

```go
http.Handle("/chat", sse.Handler(func(ctx context.Context, s *sse.Session) error {
    for token := range model.Stream(ctx, prompt) {
        if err := s.Send(ctx, sse.Text(token)); err != nil {
            return err
        }
    }
    return nil
}))
```

Sin cabeceras, sin flush, sin heartbeat, sin deadlines: `go run ./examples/00-llm-proxy`.

Con reanudación, el caso estrella — el trabajo sigue corriendo mientras el cliente
no está, y al volver continúa donde se quedó:

```go
log := sse.NewMemoryLog(sse.Retention{For: 10 * time.Minute})
http.Handle("/job", sse.Handler(sse.Follow, sse.WithLog("job", log)))
```

`go run ./examples/01-resumable-job`. Verificado sobre socket real: cliente cortado
en el paso 4, reconexión con un cursor de 27 caracteres, primer evento el paso 5.

Con topics, para multi-tenant — publicar nombra un topic concreto, suscribirse usa
filtros:

```go
b := sse.NewBroker("events", log)
http.Handle("/events", b.Handler())          // ?topic=tenant.acme.>
b.Publish(ctx, sse.MustTopic("tenant.acme.tickets"), ticket)
```

Con autorización — el `Authorizer` ve la petición entera antes de comprometer
un solo byte, y lo que devuelve *es* la suscripción:

```go
func authorize(r *http.Request) (sse.Grant, error) {
    user, ok := session(r)                       // cookie o query: EventSource no manda cabeceras
    if !ok {
        return sse.Grant{}, sse.Unauthorized("sign in to subscribe")
    }
    return sse.Grant{
        Identity: user.ID,
        Filters:  []sse.Filter{sse.MustFilter("tenant." + user.Tenant + ".>")},
        Denied:   refused,                       // se le dice, no se le oculta
        Deadline: user.TokenExpiry,              // caduca -> reconecta con credenciales frescas
    }, nil
}
```

`go run ./examples/02-multi-tenant` y `go run ./examples/03-dashboard`.

En varios nodos — **la única línea que cambia en toda la aplicación**:

```go
// log0 := sse.NewMemoryLog(retention)
log0, err := redislog.New(ctx, rdb, "sse:events", retention)
```

Ni el authorizer, ni los topics, ni el handler, ni la política de contrapresión.
Verificado con dos procesos: el cliente se corta en el nodo A en el seq 9,
reconecta contra el **nodo B**, que nunca lo había visto, y continúa en el seq 10
sin huecos. Sin sesiones pegajosas: el cursor nombra un log y un offset, no un
nodo. `go run ./examples/04-distributed`.

Declarando lo que emite el stream, una vez, y derivando de ahí el evento de
capacidades y el documento de API:

```go
var TicketCreated = sse.Declare[Ticket]("ticket.created").OnTopic("tenant.*.tickets")

b := sse.NewBroker("events", log, sse.WithCatalog(sse.NewCatalog(TicketCreated)))
TicketCreated.Publish(ctx, b, topic, ticket)   // no compila con otro tipo
```

`openapi.Generate` produce OpenAPI **3.2** con `itemSchema` y un `oneOf`
discriminado por `event: {const: …}` — el patrón que la propia especificación
publica para un stream con varios tipos de evento. Incluye los eventos
reservados de la librería, porque un cliente que no sabe qué es `sse.gap` lo
ignora justo cuando más lo necesita.

### Medido

| | |
|---|---|
| Codificación de evento | **0 allocs/op** |
| Coincidencia de filtro | **0 allocs/op**, 17–33 ns |
| Coste de difusión | **1 alloc/op con 1 suscriptor y con 1000** |
| Cursor de un solo log | **31 bytes** |

Especificación verificada contra la fuente normativa (WHATWG HTML §9.2); las
correcciones a `02` y `04` están anotadas en `06`.


---

## Frameworks

**Gin y Echo no necesitan adaptador.** Ambos envuelven `http.ResponseWriter` e
implementan `Unwrap() http.ResponseWriter`, así que `http.ResponseController`
alcanza el writer real y tanto el vaciado como los deadlines de escritura
funcionan tal cual:

```go
r.GET("/events", gin.WrapH(sse.Handler(stream)))          // Gin 1.12
e.GET("/events", echo.WrapHandler(sse.Handler(stream)))   // Echo 4.15
```

Verificado en CI (`compat/`, módulo aparte para que sus dependencias no entren
en el grafo del núcleo). Si algún día dejan de exponer el writer, el test falla
y la librería rechaza el stream con un error que nombra el tipo culpable en vez
de abrir un stream que nunca se puede vaciar.

Pendiente: **Fiber**. No va sobre `net/http` y su contexto de petición no señala
la desconexión del cliente, que es el hueco documentado del ecosistema. Necesita
una implementación propia de `Transport`.

## No-objetivos

Declarados en voz alta, porque poner los límites por delante evita expectativas
mal puestas:

- **Comunicación bidireccional de baja latencia.** Juegos, edición colaborativa
  con CRDT, voz o vídeo. Eso es WebSocket o WebRTC.
- **Transporte de datos binarios de volumen.** El cable es texto UTF-8;
  codificar en base64 cuesta ~33% de sobrecarga.
- **Garantías transaccionales o colas durables por consumidor.** Con historial
  esto ofrece entrega **al menos una vez dentro de una ventana de retención**,
  no durabilidad. Quien necesite lo segundo necesita una cola, y SSE será solo
  el último tramo de entrega.
- **Miles de mensajes por segundo por conexión individual.** El agrupamiento y
  la fusión ayudan, pero la sobrecarga del formato de texto pesa.
- **Ser un servidor autónomo.** Para eso están Centrifugo, AnyCable o el hub de
  Mercure. Esto es una librería para embeber.
- **Cliente Go.** Fuera del alcance de la v1. El decodificador de `wire/` **no**
  es un cliente: no tiene reconexión, ni backoff, ni gestión de cursor.
