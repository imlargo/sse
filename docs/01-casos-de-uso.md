# 01 — Casos de uso

Los casos están agrupados por **nivel de complejidad conceptual que exigen al
usuario**. El agrupamiento no es cosmético: es un requisito de diseño que un caso
de nivel bajo no obligue a aprender ni instanciar la maquinaria de un nivel alto.

Cada nivel debe ser **aditivo**: subir de nivel añade conceptos, nunca obliga a
reescribir lo que ya funcionaba.

---

## Nivel 0 — Un cliente, un stream

**Superficie conceptual mínima.** Sin difusión, sin topics, sin registro de
suscriptores. Un handler HTTP que produce eventos para el cliente que se conectó.

Se estima que es el caso más frecuente en descargas y el punto de entrada de la
mayoría de usuarios. **Debe ser el "hola mundo" de la librería.**

- **Streaming de tokens de modelos de lenguaje y agentes.** Generación propia o
  proxy hacia un proveedor externo. Es el caso de mayor crecimiento actual.
- **Transporte de MCP (Model Context Protocol).** Relevante porque MCP hace
  streaming SSE **sobre POST**, no sobre GET. Impone que la librería no asuma el
  método HTTP.
- **Progreso de trabajos largos**: importación de ficheros, renderizado,
  migraciones, generación de reportes, exportaciones.
- **Logs en vivo** de un build, un despliegue o un contenedor.
- **Resultados incrementales** de una búsqueda o de una consulta costosa.

Observación importante: este nivel es de **superficie de API mínima pero puede
exigir infraestructura máxima**. El caso de reanudar un stream de LLM tras una
recarga de página es un solo cliente, sin topics y sin fan-out, pero necesita
historial persistente y reanudación entre nodos. El diseño no debe asumir que
"simple en API" implica "simple en infraestructura".

---

## Nivel 1 — Difusión a un canal

Se añade el registro de suscriptores y el fan-out. Sin topics todavía: todos los
conectados reciben lo mismo.

- Dashboards de métricas en vivo y páginas de estado.
- Tickers: precios, marcadores deportivos, resultados electorales, subastas.
- Contadores de presencia agregada ("N personas viendo esto").
- Feeds públicos de actividad. El feed de cambios recientes de Wikimedia es el
  ejemplo canónico de SSE público operando a escala.
- Señales de invalidación de caché o de recarga enviadas a todos los clientes.

---

## Nivel 2 — Topics, autorización e historial

El nivel donde la librería se diferencia de lo que ya existe.

- **Notificaciones por usuario.** El caso que Medusa cubre hoy, pero resuelto bien.
- **SaaS multi-tenant**: eventos segmentados por organización, proyecto o
  workspace, con autorización aplicada en el momento de la suscripción.
- **Sincronización de estado**: cambios de entidades empujados a los clientes
  suscritos a esas entidades. Es el caso que popularizaron Mercure y API Platform.
- **Colaboración de baja frecuencia**: presencia, indicadores de "alguien está
  editando esto", movimiento de tarjetas en un tablero, comentarios nuevos.
- **Chat, en el lado de recepción.** El envío de mensajes va por POST convencional.
  Es una arquitectura sana y considerablemente más simple que WebSockets.
- **Interfaces hipermedia**: htmx, Datastar, Turbo Streams. El servidor empuja
  **fragmentos de HTML**, no JSON. Es un caso en crecimiento y prácticamente
  ninguna librería Go lo contempla.
- **Telemetría IoT hacia dashboards.** Unidireccional por naturaleza.
- Feeds de auditoría y de eventos de seguridad segmentados por tenant.

---

## Nivel 3 — Distribuido

Todos los casos anteriores, con más de un nodo servidor: autoescalado, despliegues
progresivos sin cortar conexiones, tolerancia a la caída de un nodo.

- **Reanudación de streams de LLM entre nodos.** Un cliente empieza a recibir
  tokens desde el nodo A, recarga la página o pierde la red, reconecta contra el
  nodo B y continúa donde estaba mientras la generación siguió corriendo sin
  interrupción. Hoy todo el mundo resuelve esto a mano con Redis y de forma
  acoplada a su framework. Es el caso estrella de diferenciación.
- Cualquier caso de nivel 1 o 2 detrás de un balanceador con varias réplicas.

---

## Requisito derivado: ejemplos del repositorio

Debe haber exactamente **cinco ejemplos ejecutables** en el repositorio, uno por
caso representativo, y son parte del criterio de aceptación del diseño:

1. Proxy de streaming de un LLM (nivel 0).
2. Progreso de un trabajo largo con reanudación tras desconexión (nivel 0 + historial).
3. Notificaciones multi-tenant con autorización por topic (nivel 2).
4. Dashboard de métricas con alta frecuencia de actualización (nivel 1, ejercita
   la política de contrapresión).
5. El mismo caso 3, escalado a varios nodos (nivel 3).

---

## No-objetivos

Casos que la librería **no** debe intentar cubrir. Deben aparecer explícitamente en
el README del proyecto: declarar los límites en voz alta gana credibilidad y evita
issues de expectativas mal puestas.

- **Comunicación bidireccional de baja latencia**: juegos en tiempo real, edición
  colaborativa con CRDT, voz o vídeo. Eso es WebSocket o WebRTC.
- **Transporte de datos binarios de volumen.** El cable es texto UTF-8; codificar
  en base64 cuesta aproximadamente un 33% de sobrecarga.
- **Garantías transaccionales o colas durables por consumidor.** Con historial, SSE
  puede ofrecer entrega al menos una vez dentro de una ventana de retención, no
  durabilidad. Quien necesite lo segundo necesita una cola, y SSE será solo el
  último tramo de entrega.
- **Miles de mensajes por segundo por conexión individual.** El agrupamiento y la
  fusión de eventos ayudan, pero la sobrecarga del formato de texto pesa.
- **Ser un servidor autónomo.** Existen productos que resuelven eso (Centrifugo,
  AnyCable, el hub de Mercure). Esto es una librería para embeber.
