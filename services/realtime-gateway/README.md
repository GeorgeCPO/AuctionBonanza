# Realtime Gateway

Independently buildable and testable foundation for the realtime gateway.

Its initial responsibility is consuming committed auction events and delivering client updates over Server-Sent Events. It must not decide whether a bid is valid or become the source of auction truth.

The current entry point is intentionally minimal. Event consumption, SSE
delivery, and runtime configuration will be added in later milestones.

The proposed relay/edge design is in
[`docs/REALTIME_GATEWAY.md`](../../docs/REALTIME_GATEWAY.md).
