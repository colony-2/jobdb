# Storage cutover

The named-cell release requires a fresh JobDB execution store. Pre-cell jobs,
history, artifacts, schedules, reservations, and leases are discarded. There is
no supported conversion or replay of earlier execution data.

The previous chapter-artifact conversion command and implementation have been
removed. Follow the [coordinated hard-break cutover](SPEC-Tenant-Cells.md#coordinated-hard-break-cutover).
Git repositories and administrative source configuration remain inputs to fresh
bootstrap. Create each tenant's root with `jobdb cell create-root`.
