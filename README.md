# Data platform ingestion

Ansible glue role for a validated Apache Pulsar VM and a remote HTTPS dCache
service. Runs an unprivileged systemd consumer on the Pulsar VM. Requires the
scoped client token and public certificate installed by ansible-role-apache-pulsar.
Installs its own pinned client in `/opt/data-platform-ingest` with the system
`/usr/bin/python3`, so the service does not depend on IM's Python environment.
The dCache CA must come from a trusted deployment
channel; TLS and hostname verification remain enabled.

The dedicated topic is `persistent://platform/events/data-platform`, subscription
`dcache-ingest`. Raw message bytes are stored under `/data/pulsar-events/`.
The filename is SHA256(topic + NUL + `event:` + event_id), with `.bin` extension.
Without an `event_id` property it uses `message:` + the serialized Pulsar message
ID. Repeated IDs with equal content reuse the existing object; conflicting
content is rejected and retained for redelivery. Producers must use unique event
IDs. Use this topic only for this ingestion contract.

The consumer uploads a temporary object, reads it back, performs a WebDAV MOVE
with Overwrite F and verifies the final bytes before acknowledging the message.
Storage errors cause negative acknowledgement and redelivery. Delivery is at
least once; stable names make equal-content retries idempotent. This does not
claim exactly-once transactions. A hard crash may leave `.part` objects requiring
operator cleanup. Payloads are held in memory: this minimal consumer is for small
events, not large data transfers. No payload size limit or dead letter policy is
implemented; conflicting or persistently invalid messages need operator action.

Configuration and scoped credentials: `/etc/data-platform-ingest/` (root-owned,
service group readable). Status counters: `/var/lib/data-platform-ingest/status.json`.
Service: `data-platform-ingest`. Signing secrets and admin tokens are not copied
to the service account. The current dCache deployment uses its existing WebDAV
account; introduce a dedicated storage identity before sharing with other users.

Required inputs: `ingest_dcache_host`, `ingest_dcache_password`,
`ingest_dcache_ca_pem`. See defaults for the remaining inputs. Normal convergence
tests binary equality and duplicate delivery, deleting only its own fixtures.
`ingest_test_recovery: true` additionally uses a temporary incorrect bridge
password and restarts Pulsar to verify retry and durable pending-event recovery.
Use fault tests on reserved test deployments. The verifier needs root; the
running consumer never reads the admin token.

Local tests: `python -m unittest discover -s tests -v`.
