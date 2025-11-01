# System Patterns

## System architecture:
- Client-server architecture for data submission.
- Python script acts as a client to an HTTP function emulator.

## Key technical decisions:
- Use of `requests` library for HTTP communication.
- JSON format for data payload.

## Design patterns in use:
- Request-response pattern for client-server interaction.

## Component relationships:
- `test_save_arbitrage.py` (client) interacts with the HTTP function emulator (server).

## Critical implementation paths:
- Data serialization (Python dict to JSON).
- HTTP POST request execution.
- Error handling for network and API responses.
