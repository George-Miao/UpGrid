---
title: Send notifications
description: Deliver availability transitions to Telegram, SMTP email, and webhook Channels.
---

Notification Channels receive availability transitions for service Targets and Cluster Node Targets. UpGrid supports Telegram Bot API, SMTP email, and webhooks.

## Store credentials as Secrets

Create reusable Secrets from **Overview**. Their plaintext values are encrypted before replication and are write-only: the API lists each Secret's name and ID but never returns its value.

Webhook headers can reference a reusable Secret through the HTTP API. Use a `secret_id` object instead of a literal header value:

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

See the [HTTP API reference](/reference/api/) for the complete Channel request.

## Telegram

Create a Telegram Channel with a descriptive name, a Bot API token, and the destination chat ID. When the Channel is created, UpGrid encrypts the token as an automatically managed Secret in the same replicated operation. A **Test send** uses the token currently entered in the form without storing it.

Transition messages identify the Target and whether it moved up or down.

## SMTP email

Create an SMTP Channel with a server host, port, sender, and recipient. Choose **STARTTLS** for explicit TLS upgrade, **Implicit TLS** for SMTP-over-TLS, or **Plaintext** only for a trusted local relay. UpGrid requires certificate validation for both TLS modes.

Authentication is optional. Supply a username and password together to enable it. UpGrid encrypts the password as an automatically managed, write-only Secret; a **Test send** uses the password currently entered without storing it.

## Webhook

Create a webhook Channel with its destination URL and optional literal or Secret-backed headers. UpGrid sends transition data to the endpoint and treats delivery separately from the evaluation that created the alert.

## Edit a Channel

Use **Edit** on the Alerts page to change a Channel's name, destination, and default status. A Channel's type cannot be changed.

Telegram tokens and SMTP passwords remain write-only. Leave either credential field blank while editing to keep its current automatically managed Secret, or enter a new value to replace it. Clear an SMTP username to disable authentication. When updating a webhook through the HTTP API, omit `headers` to preserve its existing literal and Secret-backed headers.

## Default Channels

Mark a Channel as **Default** to route all Targets to it unless a Target explicitly opts out. In a Target form, **Use default channels** selects and locks the current default Channels while still allowing extra Channels.

Notification delivery is at least once. Receivers should tolerate the same transition being delivered more than once after retries or failover.
