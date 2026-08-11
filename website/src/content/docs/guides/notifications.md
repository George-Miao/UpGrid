---
title: Send notifications
description: Deliver availability transitions to Telegram and webhook Channels.
---

Notification Channels receive availability transitions for service Targets and Cluster Node Targets. UpGrid currently supports Telegram Bot API and webhooks.

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

## Webhook

Create a webhook Channel with its destination URL and optional literal or Secret-backed headers. UpGrid sends transition data to the endpoint and treats delivery separately from the evaluation that created the alert.

## Default Channels

Mark a Channel as **Default** to route all Targets to it unless a Target explicitly opts out. In a Target form, **Use default channels** selects and locks the current default Channels while still allowing extra Channels.

Notification delivery is at least once. Receivers should tolerate the same transition being delivered more than once after retries or failover.
