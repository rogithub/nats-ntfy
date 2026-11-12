# nats-ntfy

Puente que escucha un consumer de NATS JetStream y reenvía cada mensaje a [ntfy](https://ntfy.sh).

La idea: **NATS entre programas, ntfy hacia personas**. Un programa que quiere avisarle algo a
una persona publica en NATS; no necesita el token de ntfy ni saber dónde está.

## Contrato

- **Subject** `avisos.<topic>` → topic `<topic>` de ntfy. Ejemplo: `avisos.papeleria` → `papeleria`.
- **Cuerpo**: el [JSON de publicación de ntfy](https://docs.ntfy.sh/publish/#publish-as-json)
  sin `topic` (lo pone el puente a partir del subject):

  ```json
  {"title": "Alta: Juan", "message": "55 1234 5678 · código 4821",
   "actions": [{"action": "view", "label": "Enviar por WhatsApp", "url": "https://wa.me/..."}]}
  ```

- **Ack** solo cuando ntfy contesta 2xx. Red caída, 5xx o 429 → NAK y NATS lo vuelve a dar en
  30 s. JSON roto o 4xx → se termina (`Term`) y queda en el log.

## Variables de entorno

| Variable | Requerida | Default |
|---|---|---|
| `NATS_USUARIO` / `NATS_PASSWORD` | Sí | — |
| `NATS_URL` | No | `nats://nats.nats.svc.cluster.local:4222` |
| `NATS_STREAM` | No | `AVISOS` |
| `NATS_CONSUMER` | No | `ntfy` |
| `NTFY_TOKEN` | Sí | — (usuario de ntfy con permiso de escritura) |
| `NTFY_URL` | No | `http://ntfy.ntfy.svc.cluster.local` |
| `RUST_LOG` | No | `nats_ntfy=info,async_nats=warn` |

## Stream y consumer

El puente no los crea; se crean una vez con el CLI de NATS:

```bash
nats stream add AVISOS --defaults --subjects "avisos.>" --storage file --retention limits \
  --max-age 1d --max-bytes 16MB --replicas 1 --discard old
nats consumer add AVISOS ntfy --defaults --pull --deliver all --ack explicit \
  --filter "avisos.>" --wait 30s --max-deliver 20
```

Permisos mínimos del usuario del puente en NATS: leer del consumer (`$JS.API.CONSUMER.INFO.AVISOS.ntfy`,
`$JS.API.CONSUMER.MSG.NEXT.AVISOS.ntfy`), confirmar (`$JS.ACK.AVISOS.ntfy.>`) y `_INBOX.>`.

## Probar

```bash
nats pub avisos.prueba '{"title":"Hola","message":"desde NATS"}'
```

## Desarrollo

```bash
cargo test
NATS_URL=nats://localhost:4222 NATS_USUARIO=... NATS_PASSWORD=... \
NTFY_URL=http://localhost:8080 NTFY_TOKEN=... cargo run
```
