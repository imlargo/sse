# 05 — Decisiones abiertas

Bifurcaciones de diseño identificadas durante el análisis. **Están deliberadamente
sin resolver.** Para cada una se da el espacio de opciones conocido y los criterios
con los que evaluarlas.

El agente debe investigar cada una, decidir, y **documentar la decisión con su
justificación**, contrastándola contra las prioridades de `00-contexto.md` y contra
los requerimientos de `03-requerimientos.md`.

Regla general para todas: cuando dos opciones empaten, decide el orden de
prioridades del autor (experiencia de desarrollo > escalado horizontal >
contrapresión > reanudación > observabilidad).

---

## D-01 · Modelo de coincidencia de topics

**Opciones conocidas:** coincidencia exacta de cadenas · comodines jerárquicos por
segmentos · selectores expresivos tipo plantilla de URI · predicados o filtros
arbitrarios definidos por la aplicación.

**Criterios:**
- Coste por publicación: la coincidencia ocurre en la ruta caliente.
- **Capacidad de delegar el filtrado a sistemas de mensajería externos.** Si el
  modelo mapea sobre los mecanismos de suscripción de los brokers habituales, cada
  nodo puede recibir solo lo que le toca en lugar de recibir todo el tráfico del
  clúster y descartarlo localmente. Con escalado horizontal en prioridad 2, esto
  pesa mucho.
- Expresividad suficiente para el caso multi-tenant (RF-B3).
- Compatibilidad con transporte en parámetros de query (RF-B7): recordar que el
  navegador trunca todo lo que sigue a `#`.
- Si se permiten comodines, decidir si se permiten también al publicar o solo al
  suscribirse, y qué implica cada opción para la previsibilidad del fan-out.
- Protección contra valores que degraden el rendimiento del mecanismo de
  coincidencia (RF-B6).
- Familiaridad del vocabulario para quien viene de sistemas de mensajería
  conocidos.

---

## D-02 · Tipado del payload al publicar

**Opciones conocidas:** bytes crudos · un valor sin tipo con serializador
enchufable · tipos genéricos en la superficie pública · combinaciones.

**Criterios:**
- Ergonomía en el punto de llamada (prioridad 1): cuántas líneas de ceremonia y
  cuántos errores hay que manejar por publicación.
- **Supervivencia a través de una frontera de red.** Cualquier cosa que cruce a
  otro nodo son bytes. Un parámetro de tipo que no pueda atravesar esa frontera
  contaminará las interfaces de escalado.
- **Heterogeneidad del stream.** Un mismo stream SSE lleva tipos de evento
  distintos con formas de payload distintas. Evaluar si la opción elegida modela
  eso bien o fuerza a un tipo único artificial.
- **Momento de la serialización.** Determina si RNF-1 (coste de fan-out) se puede
  cumplir. Serializar tarde y por suscriptor lo incumple.
- Compatibilidad con RF-G3: publicar contenido ya serializado debe seguir siendo
  trivial.
- Coherencia con RNF-7: sin reflexión en la ruta caliente.

---

## D-03 · Tamaño y ubicación de la costura de escalado

La decisión estructural más importante del proyecto, y la que RF-H5 deja
explícitamente abierta.

**Pregunta:** ¿qué es exactamente lo que se sustituye para pasar de un nodo a
varios?

**Criterios:**
- **Cuánto tiene que reimplementar quien escriba una integración externa.** Ver la
  observación sobre `tmaxmax/go-sse` en `04-investigacion-previa.md`: una costura
  demasiado grande obliga a cada integración a reimplementar el registro de
  suscripciones, la coincidencia de topics, las colas y la contrapresión — es
  decir, todo aquello donde vive la calidad de la librería, y cada implementación
  lo hará distinto y peor.
- **Viabilidad de contribuciones de la comunidad.** Una integración que se escribe
  en un rato es una integración que alguien contribuye. Una que exige reimplementar
  el núcleo, no.
- Coste cero cuando no se usa (RNF-4): la abstracción de distribución no debe
  costar nada en el caso de un solo nodo.
- **Necesidad o no de sesiones pegajosas.** Si un cliente que reconecta puede caer
  en cualquier nodo y recuperar correctamente, se elimina una restricción
  operativa importante. Es una propiedad que merece perseguirse.
- Qué garantías de orden puede sostener cada opción (RNF-12).

---

## D-04 · Esquema del identificador de reanudación

**El problema:** el identificador es opaco para el cliente, pero el servidor
necesita poder resolverlo a una posición. Y una conexión con varios topics tiene un
solo identificador (ver `02-dominio-y-restricciones.md`).

**Opciones conocidas:** codificar toda la información de posición dentro del propio
identificador · guardar la posición en el servidor y usar el identificador como
referencia · forzar un único registro ordenado global para que baste un escalar.

**Criterios:**
- Debe resolver el caso de múltiples topics (RF-C3), no fingirlo.
- Debe permitir detectar identificadores de una generación anterior (RF-C5).
- **Presupuesto de tamaño** (RF-C12): viaja en cabecera HTTP, con límites de
  infraestructura de 4 a 8 KB, y debe ser seguro para cabeceras y para URLs.
- **Impacto sobre el escalado.** Una opción que exija estado compartido por sesión
  reintroduce sesiones pegajosas. Una que fuerce un registro ordenado global impide
  particionar.
- Debe poder evolucionar sin romper clientes con identificadores antiguos
  guardados.
- Comportamiento cuando no se puede cumplir el presupuesto: RF-C12 exige declarar
  la incapacidad, no degradarse en silencio.

---

## D-05 · Relación entre historial y difusión

RF-C8 exige que el historial sea usable sin difusión, porque el caso de reanudación
de streams de LLM es un solo cliente con almacenamiento durable.

**A decidir:** de qué depende el historial y de qué depende la sesión, de forma que
el caso "un cliente + reanudación" no arrastre maquinaria de fan-out, y el caso
"difusión + historial" siga siendo coherente.

**Criterio adicional:** quién asigna los identificadores de evento. Es una decisión
con consecuencias directas sobre D-04 y sobre si el esquema funciona igual en un
nodo y en varios.

---

## D-06 · Granularidad de la retención

RF-C9 exige retención configurable; RF-C10 plantea retención diferenciada.

**A decidir:** si la retención se define por topic, por tipo de evento, por ambos, o
de forma global. Si se implementa el catálogo de eventos de RF-G6, la declaración
podría marcar qué eventos son reproducibles, lo que abre la posibilidad de retener
de forma distinta según el tipo. Evaluar si esa expresividad justifica su coste.

---

## D-07 · Forma del punto de decisión previo a la conexión

RF-F1 exige un punto donde la aplicación autoriza y configura la suscripción con
acceso a la petición completa.

**Referencia conceptual:** el mecanismo de inyección de dependencias de FastAPI
resuelve exactamente esta forma —un valor derivado por petición, componible,
sustituible en pruebas, y capaz de rechazar la petición con un código de estado—.
Su implementación se apoya en introspección en tiempo de ejecución, lo cual está
descartado (RNF-7).

**A decidir:** qué forma toma ese mecanismo en Go de manera idiomática, sin
reflexión, manteniendo la componibilidad y la sustituibilidad en pruebas.

---

## D-08 · Organización del código y de los módulos

**Restricciones que la acotan:** RF-H1 (núcleo sin dependencias), RF-H2 (agnóstico
de framework), RNF-4 (coste cero de lo no usado), RNF-5 (superficie conceptual
mínima), y el hecho de que la capa de formato de cable tiene valor por sí sola para
quien solo quiere producir o consumir el flujo.

**A decidir:** cuántas unidades públicas hay, cómo se reparten los conceptos entre
ellas, y qué mecanismo aísla las dependencias externas de los adaptadores e
integraciones. Considerar que en Go las fronteras de módulo y las de paquete
resuelven problemas distintos.

---

## D-09 · Semántica exacta de las políticas de contrapresión

RF-D2 fija el conjunto mínimo de políticas. Queda abierto:

- Qué política es la predeterminada, y si debe haber una o si la elección debe ser
  obligatoria y explícita.
- Cómo se define la clave en la política de fusión.
- Si se admiten políticas compuestas o escalonadas.
- Si la política se fija al suscribirse o puede cambiar en vivo.
- Cómo se relaciona con RF-D6: qué hace la librería cuando se elige una política de
  descarte agresivo sin historial activado.

---

## D-10 · Nombre del proyecto

Sin definir. Debe ser buscable, no colisionar con las librerías existentes del
ecosistema, y funcionar bien como nombre de import en Go.

---

## Decisiones que NO están abiertas

Cerradas por el autor. No reabrir sin consultarle:

- **v1 = solo servidor.** Sin cliente Go, sin binario autónomo.
- **La librería es dueña de sus bucles.** El usuario no escribe bucles de
  escritura ni establece cabeceras a mano.
- **El núcleo no arrastra dependencias externas.**
- **La distribución es sustituible por composición**, con comportamiento por
  defecto funcional en un solo nodo y sin dependencias. Lo abierto es *cómo* se
  corta esa costura (D-03), no *si* debe existir.
- **El orden de prioridades** de `00-contexto.md`.
- **Los no-objetivos** de `01-casos-de-uso.md`.
