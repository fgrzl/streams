[![ci](https://github.com/fgrzl/kv/actions/workflows/ci.yml/badge.svg)](https://github.com/fgrzl/kv/actions/workflows/ci.yml)
[![Dependabot Updates](https://github.com/fgrzl/kv/actions/workflows/dependabot/dependabot-updates/badge.svg)](https://github.com/fgrzl/kv/actions/workflows/dependabot/dependabot-updates)

# Streams
[](url)
#### Organization of Streams in Streams: Spaces and Segments
Streams organizes data streams into a hierarchical structure using **Spaces** and **Segments** to efficiently manage and consume events. Hopefully t[](url)his structured approach enables Streams to handle high-throughput event processing while providing flexibility for various consumption patterns.

### Spaces
A **Space** is a high-level logical container for related streams. It groups multiple streams that share a common purpose, making data management and consumption easier.

- Spaces serve as the top-level abstraction in Streams's data organization.  
- They enable broad categorization, such as by application domain, data type, or service.  
- Consumers can subscribe to an entire Space to receive interleaved data from all its Segments.

### Segments 
Within each Space, multiple **Segments** function as independent, ordered data streams.

- A Segment is a sequence of entries that maintain ordering guarantees.  
- Segments act as sub-streams under a given Space.  
- They allow parallel consumption, improving scalability and performance.  
- Each Segment has a unique identifier within its Space.

### How It Works Together  
- **Producing Data**: Data producers write records into a specific **Segment** within a **Space**.  
- **Consuming Data**: Consumers can either subscribe to an entire **Space** (receiving interleaved data from all Segments) or to a specific **Segment** (ensuring strict ordering within a stream).  
- **Peeking**: The latest entry in a stream can be retrieved from a given Segment without consuming it.  
- **Offsets & Transactions**: Consumers maintain offsets for tracking progress, and transactions ensure consistency across writes.  
