---
title: Send notifications
description: Deliver availability transitions to Telegram and webhook Channels.
---

Notification Channels receive availability transitions for service Targets and Cluster Node Targets. UpGrid currently supports Telegram Bot API and webhooks.

## Store credentials as Secrets

Create reusable Secrets from **Overview** before configuring credentials such as bot tokens or authorization headers. Secret values are write-only in the API and encrypted in replicated state.

## Telegram

Create a Telegram Channel with:

- a descriptive name;
- a Bot API token, preferably referenced through a Secret; and
- the destination chat ID.

Use **Test send** in the Channel form before saving. Transition messages identify the Target and whether it moved up or down.

## Webhook

Create a webhook Channel with its destination URL and optional headers. UpGrid sends transition data to the endpoint and treats delivery separately from the evaluation that created the alert.

## Default Channels

Mark a Channel as **Default** to route all Targets to it unless a Target explicitly opts out. In a Target form, **Use default channels** selects and locks the current default Channels while still allowing extra Channels.

Notification delivery is at least once. Receivers should tolerate the same transition being delivered more than once after retries or failover.
