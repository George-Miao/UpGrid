use std::time::Duration;

use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use snafu::ResultExt;
use upgrid_config::Cipher;
use upgrid_raft::domain::{
    Alert, AlertKind, ApplicationState, ConfigValue, SecretId, SmtpSecurity,
};

use super::{
    ChannelError, ChannelTarget, Delivery, EmailAddressSnafu, EmailSnafu, SmtpTransportSnafu,
    alert_text, resolve_value,
};

pub(crate) struct Request {
    transport: SmtpTransport,
    message: Message,
}

pub(super) struct Smtp<'a> {
    host: &'a str,
    port: u16,
    security: SmtpSecurity,
    username: Option<&'a str>,
    password: Option<SecretId>,
    from: &'a str,
    to: &'a str,
}

impl<'a> Smtp<'a> {
    pub(super) fn new(
        host: &'a str,
        port: u16,
        security: SmtpSecurity,
        username: Option<&'a str>,
        password: Option<SecretId>,
        from: &'a str,
        to: &'a str,
    ) -> Self {
        Self {
            host,
            port,
            security,
            username,
            password,
            from,
            to,
        }
    }
}

impl ChannelTarget for Smtp<'_> {
    fn request(
        &self,
        state: &ApplicationState,
        cipher: &Cipher,
        alert: &Alert,
    ) -> Result<Delivery, ChannelError> {
        let password = self
            .password
            .map(|id| resolve_value(state, cipher, &ConfigValue::Secret(id)))
            .transpose()?;
        let subject = match alert.id.kind {
            AlertKind::Down => format!("UpGrid: {} is down", alert.target_name),
            AlertKind::Recovered => format!("UpGrid: {} recovered", alert.target_name),
        };
        request(
            self.host,
            self.port,
            self.security,
            self.username,
            password.as_deref(),
            self.from,
            self.to,
            &subject,
            &alert_text(alert),
        )
        .map(Delivery::Smtp)
    }
}

pub(super) fn test_request(
    host: &str,
    port: u16,
    security: SmtpSecurity,
    username: Option<&str>,
    password: Option<&str>,
    from: &str,
    to: &str,
) -> Result<Request, ChannelError> {
    request(
        host,
        port,
        security,
        username,
        password,
        from,
        to,
        "UpGrid notification channel test",
        "UpGrid notification channel test",
    )
}

pub(crate) fn send(request: Request) -> Result<(), lettre::transport::smtp::Error> {
    request.transport.send(&request.message).map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn request(
    host: &str,
    port: u16,
    security: SmtpSecurity,
    username: Option<&str>,
    password: Option<&str>,
    from: &str,
    to: &str,
    subject: &str,
    body: &str,
) -> Result<Request, ChannelError> {
    if host.trim().is_empty() || port == 0 {
        return Err(ChannelError::InvalidSmtp {
            message: "host and port must be configured",
        });
    }
    if username.is_some() != password.is_some() {
        return Err(ChannelError::InvalidSmtp {
            message: "username and password must be configured together",
        });
    }
    let builder = match security {
        SmtpSecurity::None => SmtpTransport::builder_dangerous(host),
        SmtpSecurity::StartTls => {
            SmtpTransport::starttls_relay(host).context(SmtpTransportSnafu)?
        }
        SmtpSecurity::Tls => SmtpTransport::relay(host).context(SmtpTransportSnafu)?,
    }
    .port(port)
    .timeout(Some(Duration::from_secs(15)));
    let builder = match (username, password) {
        (Some(username), Some(password)) => {
            builder.credentials(Credentials::new(username.to_owned(), password.to_owned()))
        }
        _ => builder,
    };
    let from = from.parse::<Mailbox>().context(EmailAddressSnafu)?;
    let to = to.parse::<Mailbox>().context(EmailAddressSnafu)?;
    let message = Message::builder()
        .from(from)
        .to(to)
        .subject(subject)
        .body(body.to_owned())
        .context(EmailSnafu)?;

    Ok(Request {
        transport: builder.build(),
        message,
    })
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::thread;

    use super::*;

    #[test]
    fn sends_test_message_to_plaintext_smtp_server() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.write_all(b"220 localhost ESMTP\r\n").unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut message = String::new();
            let mut data = false;
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if data {
                    if line == ".\r\n" {
                        stream.write_all(b"250 queued\r\n").unwrap();
                        data = false;
                    } else {
                        message.push_str(&line);
                    }
                } else if line.starts_with("EHLO ") {
                    stream.write_all(b"250 localhost\r\n").unwrap();
                } else if line.starts_with("MAIL FROM:") || line.starts_with("RCPT TO:") {
                    stream.write_all(b"250 OK\r\n").unwrap();
                } else if line == "DATA\r\n" {
                    stream
                        .write_all(b"354 End data with <CR><LF>.<CR><LF>\r\n")
                        .unwrap();
                    data = true;
                } else if line == "QUIT\r\n" {
                    stream.write_all(b"221 Bye\r\n").unwrap();
                    break;
                } else {
                    panic!("unexpected SMTP command: {line:?}");
                }
            }
            message
        });

        let request = test_request(
            "127.0.0.1",
            port,
            SmtpSecurity::None,
            None,
            None,
            "upgrid@example.com",
            "on-call@example.com",
        )
        .unwrap();
        send(request).unwrap();

        let message = server.join().unwrap();
        assert!(message.contains("Subject: UpGrid notification channel test"));
        assert!(message.contains("\r\n\r\nUpGrid notification channel test\r\n"));
    }
    #[test]
    fn reports_recipient_rejection_from_smtp_server() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.write_all(b"220 localhost ESMTP\r\n").unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line.starts_with("EHLO ") || line.starts_with("MAIL FROM:") {
                    stream.write_all(b"250 OK\r\n").unwrap();
                } else if line.starts_with("RCPT TO:") {
                    stream.write_all(b"550 mailbox unavailable\r\n").unwrap();
                    break;
                } else {
                    panic!("unexpected SMTP command: {line:?}");
                }
            }
        });
        let request = test_request(
            "127.0.0.1",
            port,
            SmtpSecurity::None,
            None,
            None,
            "upgrid@example.com",
            "missing@example.com",
        )
        .unwrap();

        assert!(send(request).is_err());
        server.join().unwrap();
    }
}
