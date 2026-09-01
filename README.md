# sysmon-pipeline
A high-throughput telemetry engine designed to execute network probes, cache real-time state and publish async incident streams via a decoupled Go architecture.

[Phase 1: In-Memory Go API]  --->  [Phase 2: Podman + MongoDB]  --->  [Phase 3: Redis]  --->  [Phase 4: RabbitMQ]
      (Right Now)                       (Persistent Data)               (Fast Cache)            (Async Workers)
