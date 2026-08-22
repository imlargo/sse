# 00 — Contexto general

## Qué se va a construir

Una librería de **Server-Sent Events (SSE) para Go**: reutilizable, robusta, lista
para producción y completa en funcionalidad, aplicable a múltiples casos de uso
reales sin que el usuario tenga que envolverla o parchearla.

El resultado esperado es un proyecto **open source serio y mantenible por una
comunidad**: idiomático en Go, con una superficie de API estable, documentación
tratada como parte del producto y una estrategia de pruebas que sostenga
contribuciones externas sin que cada PR sea un riesgo.

El alcance de la **v1 es solo el lado servidor**. Un cliente Go para consumir
streams SSE queda fuera de la v1 (puede volver en una versión posterior). Un hub
o binario autónomo queda fuera del alcance por completo.

## Trasfondo y motivación

### Perfil del autor

Ingeniero fullstack senior con profundidad real en Go. No necesita explicaciones
introductorias sobre concurrencia, `net/http`, interfaces o el modelo de memoria.
Espera conversación al nivel de decisiones de diseño y trade-offs, no de tutorial.

### Relación con Medusa

El autor construyó y mantiene su propio framework en Go, **Medusa**
(`github.com/imlargo/medusa`), construido sobre Gin. Medusa ya incluye una
implementación de SSE que **no le satisface**, y esta librería nace precisamente
para superarla.

La implementación actual de SSE en Medusa tiene esta forma: el usuario llama a una
función de suscripción pasando identificadores de usuario y dispositivo, obtiene un
objeto cliente, extrae de él un canal, y **escribe él mismo el bucle
`for { select { ... } }`**, pone las cabeceras HTTP a mano, formatea cada evento con
la utilidad de SSE de Gin y llama a `Flush()`.

Las carencias detectadas, que funcionan como especificación negativa de lo que hay
que lograr:

- **La abstracción está en el nivel equivocado.** Al ser el usuario quien escribe
  el bucle de escritura, la librería no puede ser responsable de nada: ni de
  heartbeats, ni de deadlines, ni de orden en la reproducción de historial, ni de
  drenaje al apagar. Cualquier mejora futura de la librería es imposible de
  aplicar sin romper a todos sus usuarios.
- **Modelo de destinatario cableado.** Está fijado a identificadores de usuario y
  dispositivo. No hay concepto de topic, canal ni sala, y por tanto no hay
  broadcast, ni difusión por grupo, ni multi-tenant.
- **Acoplamiento al framework.** Depende de Gin y del helper de eventos de Gin, lo
  que impide controlar la codificación y el agrupamiento de eventos.
- **Ausencias funcionales.** Sin soporte de `Last-Event-ID`, sin historial, sin
  política de contrapresión, sin errores de escritura observables, sin métricas.

La librería nueva **no es un refactor de la de Medusa**. Es un proyecto
independiente, agnóstico de framework, que Medusa podrá consumir como dependencia
igual que cualquier otro proyecto.

### Por qué existe el hueco

El ecosistema Go tiene librerías de SSE, pero ninguna cubre bien el conjunto
completo. Las más populares o están inactivas y con backlog de bugs sin atender, o
resuelven el formato de cable y dejan al usuario todo lo difícil, o están atadas a
un framework concreto. El detalle de qué hace cada una está en `04-investigacion-previa.md`.

## La tesis del problema

El punto de partida conceptual, que debe guiar todo lo demás:

> **La dificultad de SSE no está en el formato de cable.** El formato son unos
> pocos campos de texto y se implementa en muy poco código. Un servidor SSE de
> producción es otra cosa: **un sistema de fan-out con control de flujo por
> conexión y un contrato de reanudación.** Ahí es donde fallan las
> implementaciones existentes, y ahí es donde esta librería tiene que ser buena.

Los problemas duros, en orden de dificultad observada:

1. **Contrapresión.** Un consumidor lento no puede bloquear al publicador ni
   degradar a los demás consumidores.
2. **Reanudación.** `Last-Event-ID` es una promesa hecha al cliente. Cumplirla o
   no cumplirla es una decisión que debe ser explícita, nunca un fallo silencioso.
3. **Detección de desconexión y timeouts.** Un cliente que deja de leer no puede
   dejar recursos colgados indefinidamente.
4. **Concurrencia.** Escribir a una misma conexión desde varios sitios es
   incorrecto y debe ser imposible por construcción.
5. **Ciclo de vida.** Apagar un proceso con decenas de miles de conexiones
   abiertas sin provocar una avalancha de reconexiones simultáneas.
6. **Infraestructura hostil.** Proxies que bufferean, balanceadores que matan
   conexiones ociosas, límites de conexiones por dominio en el navegador.
7. **Autenticación.** La API `EventSource` del navegador no permite enviar
   cabeceras, y los tokens caducan mientras el stream sigue abierto.

## Referentes de calidad

Dos referencias explícitas de las que se espera que el proyecto aprenda:

**FastAPI (Python)** como referente de experiencia de desarrollo. Lo que se quiere
tomar de él: el modelo declarativo (se declara qué se quiere, el framework ejecuta
la maquinaria), la revelación progresiva de complejidad (lo simple es trivial, lo
complejo es posible, y cada escalón es aditivo y no una reescritura), la
autodocumentación derivada de las mismas declaraciones que rigen el
comportamiento, los mensajes de error que enseñan, y la documentación como parte
del producto y no como un anexo.

**Advertencia sobre este referente:** la potencia de FastAPI se apoya en
introspección en tiempo de ejecución, que es asumible en Python e inadecuada en
Go, especialmente en una ruta caliente de fan-out. Reproducir su *sensación*
declarativa es deseable; reproducir su *mecanismo* no lo es. La traducción a Go de
"declarativo" pasa por tipos y generación de código, no por reflexión. Los intentos
previos de trasplantar FastAPI a Go mediante reflexión producen librerías no
idiomáticas que la comunidad no adopta.

**Las mejores librerías de SSE existentes**, en Go y en otros lenguajes, como
referente de qué abstracciones han demostrado funcionar. Detalle en
`04-investigacion-previa.md`.

## Prioridades, ordenadas

El autor ordenó explícitamente los ejes de diferenciación. **Este orden debe
resolver los empates de diseño.**

| # | Eje | Qué significa |
|---|---|---|
| 1 | **Experiencia de desarrollo y API mínima** | Que la superficie conceptual sea pequeña, que el caso simple sea trivial y que el editor guíe al usuario sin necesidad de leer documentación. |
| 2 | **Escalado horizontal** | Que el mismo código funcione en un nodo y en muchos, y que pasar de uno a muchos sea composición, no una reescritura. |
| 3 | **Contrapresión y control de flujo** | Que el comportamiento ante consumidores lentos sea elegible, predecible y observable. |
| 4 | **Reanudación real (`Last-Event-ID`)** | Que la reanudación funcione de verdad, y que cuando no pueda funcionar lo diga. |
| 5 | **Observabilidad** | Que se pueda operar: métricas, trazas y logs útiles sin imponer dependencias. |

Nota sobre la tensión entre #1 y #2: son ejes que entran en conflicto. Una
abstracción preparada para distribución tiende a filtrarse hacia la API pública
(publicación asíncrona, ausencia de garantías globales de orden, contadores que
dejan de ser globales). El agente debe identificar dónde ocurre ese conflicto y
resolverlo de forma que **la API no mienta**: es preferible una garantía más débil
bien documentada que una API que parezca prometer más de lo que puede cumplir.

## Decisiones de alcance ya cerradas por el autor

Estas no están en discusión:

- **v1 = solo servidor.** Sin cliente Go, sin hub binario.
- **La distribución debe ser reemplazable por composición.** El comportamiento por
  defecto debe funcionar en un solo nodo sin dependencias externas, y escalar a
  varios nodos debe ser cuestión de sustituir piezas, no de reescribir la
  integración. *Cómo* se corta esa costura es una decisión abierta (ver `05`).
- **El núcleo no arrastra dependencias.** Usar la librería con la biblioteca
  estándar no debe traer Gin, Fiber, Redis, NATS, Prometheus ni OpenTelemetry.
- **La librería es dueña de sus bucles.** El usuario no escribe bucles de
  lectura ni de escritura. Este es el punto de ruptura explícito con la
  implementación actual de Medusa.

## Qué significa "terminado"

La v1 se considera lista cuando:

- Los casos de uso de nivel 0 a 3 de `01-casos-de-uso.md` están cubiertos y hay un
  ejemplo ejecutable y legible de cada uno en el repositorio.
- Los requerimientos marcados como **obligatorios** en `03-requerimientos.md` están
  implementados y verificados.
- La superficie de API pública está congelada y documentada.
- Existe una suite de conformidad contra la especificación, publicada y
  reproducible.
- Existen benchmarks reproducibles con presupuesto de asignaciones declarado.

**Criterio cualitativo de validación del diseño:** si los ejemplos de los cinco
casos de uso principales se pueden escribir cortos y legibles, el diseño es bueno.
Si alguno se vuelve feo o verboso, ahí está el defecto de diseño, y hay que
corregir la librería y no el ejemplo.
