---
title: Send notifications
description: Deliver availability transitions to Telegram, SMTP email, and webhook channels.
---

Notification channels receive availability transitions for service targets and cluster node targets. UpGrid supports Telegram bot API, SMTP email, and webhooks.

## Store credentials as secrets

Create reusable secrets from **Overview**. Their plaintext values are encrypted before replication and are write-only: the API lists each secret's name and ID but never returns its value.

Webhook headers can reference a reusable secret through the HTTP API. Use a `secret_id` object instead of a literal header value:

```json
{
  "type": "webhook",
  "name": "On-call",
  "url": "https://hooks.example.com/upgrid",
  "headers": {
    "authorization": {
      "secret_id": "0198f24c-7e91-7d50-9d74-35b71a34af10"
    }
  }
}
```

See the [HTTP API reference](/reference/api/) for the complete channel request.

## Telegram

Create a Telegram channel with a descriptive name, a bot API token, and the destination chat ID. When the channel is created, UpGrid encrypts the token as an automatically managed secret in the same replicated operation. A **Test send** uses the token currently entered in the form without storing it.

Transition messages identify the target and whether it moved up or down.

## SMTP email

Create an SMTP channel with a server host, port, sender, and recipient. Choose **STARTTLS** for explicit TLS upgrade, **Implicit TLS** for SMTP-over-TLS, or **Plaintext** only for a trusted local relay. UpGrid requires certificate validation for both TLS modes.

Authentication is optional. Supply a username and password together to enable it. UpGrid encrypts the password as an automatically managed, write-only secret; a **Test send** uses the password currently entered without storing it.

## Webhook

Create a webhook channel with its destination URL and optional literal or secret-backed headers. UpGrid sends transition data to the endpoint and treats delivery separately from the evaluation that created the alert.

## Edit a channel

Use **Edit** on the alerts page to change a channel's name, destination, and default status. A channel's type cannot be changed.

Telegram tokens and SMTP passwords remain write-only. Leave either credential field blank while editing to keep its current automatically managed secret, or enter a new value to replace it. Clear an SMTP username to disable authentication. When updating a webhook through the HTTP API, omit `headers` to preserve its existing literal and secret-backed headers.

## Default channels

Mark a channel as **Default** to route all targets to it unless a target explicitly opts out. In a target form, **Use default channels** selects and locks the current default channels while still allowing extra channels.

Notification delivery is at least once. Receivers should tolerate the same transition being delivered more than once after retries or failover.
