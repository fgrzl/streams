[![ci](https://github.com/fgrzl/kv/actions/workflows/ci.yml/badge.svg)](https://github.com/fgrzl/kv/actions/workflows/ci.yml)
[![Dependabot Updates](https://github.com/fgrzl/kv/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/fgrzl/kv/actions/workflows/dependabot/dependabot-updates)

# Streams  
[](url)

#### Organizing Streams: Spaces and Segments  
Streams uses a hierarchical model—**Spaces** and **Segments**—to efficiently manage and consume high-throughput event data.

### Spaces  
A **Space** is a top-level logical container for related streams.

- Groups streams by application, data type, or service  
- Enables broad categorization and easier management  
- Consumers can subscribe to an entire Space for interleaved events from all Segments  

### Segments  
**Segments** are independent, ordered sub-streams within a Space.

- Maintain strict event order  
- Support parallel consumption for scalability  
- Identified uniquely within their Space  

### How It Works  

- **Producing**: Data is written to specific Segments in a Space  
- **Consuming**:  
  - Subscribe to a Space for all Segments (interleaved)  
  - Subscribe to a Segment for strict ordering  
- **Peeking**: Read the latest entry in a Segment without consuming it  
- **Offsets & Transactions**:  
  - Offsets track consumer progress  
  - Transactions ensure consistent writes  
