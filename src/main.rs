//! Puente NATS → ntfy.
//!
//! "NATS entre programas, ntfy hacia personas": cualquier programa que quiera avisarle algo a
//! una persona publica en NATS, y este puente lo reenvía a ntfy. Así nadie más necesita el token
//! de ntfy.
//!
//! Contrato:
//! - Subject `avisos.<topic>` → topic `<topic>` de ntfy (`avisos.papeleria` → `papeleria`).
//! - Cuerpo: el JSON de publicación de ntfy (`title`, `message`, `priority`, `tags`, `actions`,
//!   …), sin `topic`; el puente lo pone a partir del subject y lo reenvía tal cual.
//! - Ack a NATS solo cuando ntfy contesta 2xx. Si ntfy no está o falla (5xx, 429, red), NAK con
//!   espera y NATS lo vuelve a dar. Un mensaje que nunca va a pasar (JSON roto, 4xx) se termina
//!   (`Term`) para que no se repita.
//!
//! El stream y el consumer se crean a mano con el CLI de NATS (ver README); el puente solo los lee.

use std::time::Duration;

use async_nats::jetstream::{AckKind, consumer::PullConsumer};
use futures_util::StreamExt;
use serde_json::Value;

type Error = Box<dyn std::error::Error + Send + Sync>;

const PREFIJO: &str = "avisos.";

struct Config {
    nats_url: String,
    nats_usuario: String,
    nats_password: String,
    stream: String,
    consumer: String,
    ntfy_url: String,
    ntfy_token: String,
}

impl Config {
    fn desde_entorno() -> Result<Self, String> {
        let var = |nombre: &str, default: Option<&str>| match (std::env::var(nombre), default) {
            (Ok(v), _) if !v.is_empty() => Ok(v),
            (_, Some(d)) => Ok(d.to_string()),
            _ => Err(format!("falta la variable de entorno {nombre}")),
        };
        Ok(Config {
            nats_url: var("NATS_URL", Some("nats://nats.nats.svc.cluster.local:4222"))?,
            nats_usuario: var("NATS_USUARIO", None)?,
            nats_password: var("NATS_PASSWORD", None)?,
            stream: var("NATS_STREAM", Some("AVISOS"))?,
            consumer: var("NATS_CONSUMER", Some("ntfy"))?,
            ntfy_url: var("NTFY_URL", Some("http://ntfy.ntfy.svc.cluster.local"))?,
            ntfy_token: var("NTFY_TOKEN", None)?,
        })
    }
}

/// Qué hacer con un mensaje después de intentar reenviarlo.
enum Resultado {
    /// ntfy lo aceptó: ack.
    Entregado,
    /// Falla pasajera: NAK y NATS lo vuelve a dar en un rato.
    Reintentar(String),
    /// Nunca va a pasar: se termina para que no se repita.
    Descartar(String),
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nats_ntfy=info,async_nats=warn".into()),
        )
        .init();

    let config = match Config::desde_entorno() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("{e}");
            std::process::exit(1);
        }
    };
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("cliente HTTP");

    // Si se cae la conexión o el consumer no existe todavía, vuelve a empezar. SIGTERM (k3s al
    // reiniciar el pod) corta el ciclo; lo que no alcanzó a confirmarse, NATS lo vuelve a dar.
    tokio::select! {
        _ = async {
            loop {
                if let Err(e) = escuchar(&config, &http).await {
                    tracing::warn!("NATS: {e}; reintento en 10 s");
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        } => {}
        _ = apagado() => tracing::info!("apagando"),
    }
}

async fn apagado() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("señal SIGTERM");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

async fn escuchar(config: &Config, http: &reqwest::Client) -> Result<(), Error> {
    let cliente = async_nats::ConnectOptions::with_user_and_password(
        config.nats_usuario.clone(),
        config.nats_password.clone(),
    )
    .name("nats-ntfy")
    .connect(&config.nats_url)
    .await?;
    let js = async_nats::jetstream::new(cliente);
    let consumer: PullConsumer = js
        .get_consumer_from_stream(&config.consumer, &config.stream)
        .await?;
    let mut mensajes = consumer.messages().await?;
    tracing::info!("escuchando NATS ({} > {})", config.stream, config.consumer);

    while let Some(mensaje) = mensajes.next().await {
        let mensaje = mensaje?;
        let subject = mensaje.subject.to_string();
        match reenviar(config, http, &subject, &mensaje.payload).await {
            Resultado::Entregado => {
                tracing::info!("{subject}: entregado a ntfy");
                mensaje.ack().await?;
            }
            Resultado::Reintentar(e) => {
                tracing::warn!("{subject}: {e}; NATS lo vuelve a dar en 30 s");
                mensaje
                    .ack_with(AckKind::Nak(Some(Duration::from_secs(30))))
                    .await?;
            }
            Resultado::Descartar(e) => {
                tracing::error!(
                    "{subject}: {e}; se descarta: {:?}",
                    String::from_utf8_lossy(&mensaje.payload)
                );
                mensaje.ack_with(AckKind::Term).await?;
            }
        }
    }
    Ok(())
}

async fn reenviar(config: &Config, http: &reqwest::Client, subject: &str, payload: &[u8]) -> Resultado {
    let Some(topic) = topic_de(subject) else {
        return Resultado::Descartar(format!("el subject no es {PREFIJO}<topic>"));
    };
    let mut cuerpo = match serde_json::from_slice::<Value>(payload) {
        Ok(Value::Object(cuerpo)) => cuerpo,
        Ok(_) => return Resultado::Descartar("el cuerpo no es un objeto JSON".into()),
        Err(e) => return Resultado::Descartar(format!("JSON inválido: {e}")),
    };
    // El topic lo decide el subject, no el cuerpo: así los permisos de NATS por subject son
    // también los permisos por topic de ntfy.
    cuerpo.insert("topic".into(), Value::String(topic.to_string()));

    // La API JSON de ntfy se publica en la raíz, con el topic dentro del cuerpo.
    let respuesta = http
        .post(&config.ntfy_url)
        .bearer_auth(&config.ntfy_token)
        .json(&cuerpo)
        .send()
        .await;
    match respuesta {
        Err(e) => Resultado::Reintentar(format!("ntfy no contesta: {e}")),
        Ok(r) if r.status().is_success() => Resultado::Entregado,
        Ok(r) => {
            let status = r.status();
            let texto = r.text().await.unwrap_or_default();
            if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                Resultado::Reintentar(format!("ntfy {status}: {texto}"))
            } else {
                Resultado::Descartar(format!("ntfy {status}: {texto}"))
            }
        }
    }
}

/// `avisos.papeleria` → `papeleria`. ntfy solo acepta letras, números, `-` y `_` en el topic.
fn topic_de(subject: &str) -> Option<&str> {
    let topic = subject.strip_prefix(PREFIJO)?;
    let valido = !topic.is_empty()
        && topic.len() <= 64
        && topic.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    valido.then_some(topic)
}

#[cfg(test)]
mod tests {
    use super::topic_de;

    #[test]
    fn topic_desde_subject() {
        assert_eq!(topic_de("avisos.papeleria"), Some("papeleria"));
        assert_eq!(topic_de("avisos.papeleria-alertas"), Some("papeleria-alertas"));
        assert_eq!(topic_de("avisos."), None);
        assert_eq!(topic_de("avisos.a.b"), None);
        assert_eq!(topic_de("otra.cosa"), None);
    }
}
