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
| `05-decisiones-abiertas.md` | Las bifurcaciones de diseño identificadas, con el espacio de opciones y los criterios para elegir. **Deliberadamente sin resolver.** |

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

Documentos de requisitos. No hay código escrito. No hay decisiones de arquitectura
tomadas. El nombre de la librería está sin definir.
