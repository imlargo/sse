# 04 — Investigación previa

Estado del arte revisado. Cada entrada indica qué resuelve, qué aporta como
referencia y dónde queda corta. **No son recomendaciones de diseño**: son
observaciones sobre implementaciones existentes que el agente debe verificar y
ampliar por su cuenta.

Todo lo listado debe considerarse **potencialmente desactualizado**: verificar
versiones, actividad del repositorio y estado de la API antes de basar una decisión
en ello.

---

## Go

### `tmaxmax/go-sse`
La implementación más cuidada del ecosistema Go y la principal referencia a
superar.

**Aciertos:**
- Trata la mensajería como una interfaz enchufable (`Provider`: publicar,
  suscribir, apagar), con una implementación en memoria por defecto. Su
  documentación es explícita en que esa pieza es la que determina el máximo de
  clientes, la latencia entre publicar y recibir, y el throughput máximo.
- Separa la reproducción de historial en una interfaz aparte (`Replayer`), con
  implementaciones en memoria por tiempo de vida y por número finito de eventos, e
  invita explícitamente a implementar una con almacenamiento persistente.
- El servidor implementa la interfaz de handler de la biblioteca estándar pero es
  agnóstico de framework.
- Cliente y servidor completamente desacoplados.
- Distingue constructores que devuelven error de variantes que entran en pánico,
  para valores constantes.

**Dónde queda corta:**
- La política de contrapresión no es un ciudadano de primera clase.
- Modelo de topics plano.
- El historial vive dentro del proveedor de mensajería en lugar de ser un registro
  con posiciones consultables.
- **Observación de diseño relevante:** su interfaz de proveedor es de grano grueso.
  Implementarla para un sistema externo obliga a reimplementar el registro de
  suscripciones, la coincidencia de topics, las colas por suscriptor, la política
  de contrapresión y la reproducción. Es decir: cada integración externa
  reimplementa exactamente la parte donde vive la calidad de la librería.

### `r3labs/sse`
Popular e históricamente la opción por defecto. Sirve sobre todo como
contraejemplo.

- Configuración mediante campos públicos mutables en el struct del servidor
  (tiempo de vida de eventos, tamaño de buffer, creación automática de streams,
  reproducción automática). Mutable, no validable, y susceptible de carreras si se
  toca después del arranque.
- La operación de publicación bloquea hasta que el mensaje llegó a todos los
  suscriptores o el stream se cerró.
- Ofrece codificación en base64 para contenido binario.
- Sin actividad relevante desde 2023, con forks de la comunidad intentando atender
  el backlog de bugs. **Hay hueco de mercado.**

### `fibersse`
Librería reciente centrada en Fiber. Interesa por dos motivos.

**Documenta un fallo real del ecosistema:** las librerías existentes están
construidas sobre `net/http` y se rompen en Fiber porque la señal de finalización
del contexto de petición de fasthttp solo se dispara al apagar el servidor, no
cuando el cliente se desconecta, dejando suscriptores zombi que fugan memoria de
forma permanente.

**Su lista de capacidades funciona como lista de comprobación competitiva:** fusión
de eventos, carriles de prioridad, comodines en topics, regulación adaptativa,
métricas, drenaje grácil.

### `Huma`
Framework de APIs en Go con soporte de SSE. Relevante por **una idea concreta**:
registra la operación junto con un mapa de nombre de evento a struct de Go
(`"message"`, `"userCreate"`, `"mailReceived"` asociados a sus tipos), con la regla
de que cada modelo de evento sea un tipo Go único. De ahí deriva documentación
automática.

Es precedente directo de la idea de catálogo de eventos (RF-G6). Limitación: está
atado a Huma.

### Otras
`alexandrevicenzi/go-sse` y varias implementaciones menores. Verificar actividad y
alcance antes de considerarlas.

---

## Otros lenguajes

### `better-sse` (Node.js)
La mejor descomposición conceptual encontrada. Separa explícitamente:

- **Sesión**: una conexión individual.
- **Canal**: difusión a muchos.
- **Buffer de eventos**: capa aparte que escribe campos SSE crudos y conformes a
  la especificación en un buffer de texto que se manda directo por el cable.

Ofrece además agrupamiento de varios eventos en una sola transmisión, y un modelo
de creación de sesión a partir de la petición y la respuesta que es
ergonómicamente muy limpio.

### Mercure (protocolo + hub, Go)
Lo que ocurre cuando SSE se lleva a nivel de protocolo. Los publicadores mandan a
un hub; los suscriptores abren una conexión larga y declaran mediante
*coincidencia de topics* qué quieren recibir; el hub verifica autorización y
despacha, incluyendo actualizaciones privadas que solo reciben suscriptores
autorizados.

**Dos patrones a estudiar:**
- Selectores de topic expresivos en lugar de coincidencia exacta de cadenas.
- Desconexión automática cuando expira el token de autorización: el cliente
  reconecta por sí solo y, gracias a la reconciliación nativa del protocolo,
  recupera lo perdido con credenciales frescas. Convierte la expiración de token en
  un no-evento.

### FastAPI (Python)
Referente de experiencia de desarrollo del autor. **Tiene soporte nativo de SSE
desde la versión 0.135.0**, así que no hay que inferir su enfoque.

**Su modelo:** un handler que devuelve un iterable asíncrono y va emitiendo eventos
cuyo payload es un modelo tipado que se serializa automáticamente. Funciona con
cualquier método HTTP, no solo GET, precisamente para casos como MCP.

**El detalle más instructivo:** el identificador de reanudación llega al handler
como un **parámetro declarado y tipado**, no como algo que hay que extraer de las
cabeceras y parsear a mano. El estado del protocolo se declara, no se excava. En
todas las librerías Go revisadas, ese valor es algo que el usuario va a buscar y
convierte por su cuenta.

**Su límite:** el modelo es un generador por petición. Un cliente, un stream. **No
resuelve fan-out en absoluto.** Excelente referencia para el nivel 0 de casos de
uso, irrelevante para los niveles 1 a 3.

### Patrones de reanudación en streaming de LLM
El área que más ha movido el estado del arte y donde hay más dolor real.

El patrón que todo el mundo reimplementa: bufferear la salida de tokens del lado
servidor en Redis; cuando el cliente reconecta tras recargar la página, lee del
buffer y reproduce desde donde se quedó, mientras la generación sigue sin
interrupción en el servidor.

**La crítica publicada del enfoque actual es directamente aprovechable como
enunciado del problema:** la limitación de fondo es SSE mismo, que no lleva
identidad de sesión, ni protocolo de reconexión, ni fan-out; y las soluciones
existentes son de un solo dispositivo y acopladas al framework.

**Observación clave:** el mecanismo que resuelve la reanudación por `Last-Event-ID`
y el que resuelve la reanudación de un stream de LLM **son el mismo problema**.
Nadie en Go los ha empaquetado juntos.

### Productos completos
Centrifugo, AnyCable y el hub de Mercure resuelven mensajería en tiempo real como
servidores autónomos. Son referencia de funcionalidad y de vocabulario, pero
representan una categoría de producto distinta (ver no-objetivos en `01`).

---

## Formatos de descripción de API

Relevante para RF-G6.

- **OpenAPI** tiene entrada de registro para el tipo de medio `text/event-stream`,
  con consideraciones específicas para server-sent events y para tipos de medio
  secuenciales. Existe además una propuesta activa en la especificación para dar
  soporte nativo a SSE, cuyo argumento es que SSE es un protocolo distinto de
  websockets y merece que las herramientas puedan construir tooling específico.
- **AsyncAPI** se usa para describir APIs SSE reales; hay definiciones publicadas
  de APIs públicas basadas en SSE que usan su objeto de streaming.

Ambos deben evaluarse. Consideración práctica: el framework del autor (Medusa) ya
emite OpenAPI y Swagger.

---

## Fuentes a consultar directamente

- Especificación de WHATWG HTML, sección de Server-Sent Events (fuente normativa).
- Documentación de MDN sobre el uso de server-sent events y el formato del flujo
  de eventos.
- Registro de tipos de medio de la especificación OpenAPI.
- Repositorios y documentación de las librerías listadas arriba.
