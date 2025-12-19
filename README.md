## Project Overview

### What We're Building

A **Global Cargo Ship Fleet Management Platform** that demonstrates:

- **Real-Time Ship Telemetry**: Streaming sensor data (GPS, speed, fuel consumption)
- **Batch Container Tracking**: Daily cargo manifest updates
- **CDC Pipeline**: Change Data Capture for container status updates
- **SCD Type 2**: Historical ship maintenance records
- **Incremental Processing**: Watermarking, CDF, and CDC patterns
- **Near Real-Time Dashboard**: Fleet monitoring with <5 minute latency
- **Advanced PySpark**: Schema evolution, error handling, optimization

### Why This Use Case?

**Justification**: Maritime logistics involves real-time tracking (ship sensors), periodic updates (port schedules), and audit requirements (container chain of custody). It naturally demonstrates:

- High-volume streaming data (telemetry every 30 seconds)
- Late-arriving data (ships in remote areas with delayed transmissions)
- Complex change tracking (container status, ship maintenance)
- Global scale operations requiring optimization
- Compliance and lineage tracking requirements
