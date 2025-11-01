# Active Context

## Current work focus:
- Completing the `test_save_arbitrage.py` script to post arbitrage data to an emulator.
- Setting up the project-specific memory bank structure.

## Recent changes:
- Moved memory bank files from global rules directory to project-specific `memory-bank` folder.
- Created `projectbrief.md` and `productContext.md` in the new `memory-bank` directory.

## Next steps:
- Create remaining core memory bank files (`systemPatterns.md`, `techContext.md`, `progress.md`).
- Implement the HTTP POST request logic in `test_save_arbitrage.py`.
- Execute `test_save_arbitrage.py` to send data to the emulator.

## Active decisions and considerations:
- Ensuring the JSON data structure in `test_save_arbitrage.py` matches the emulator's expected input.
- Implementing robust error handling for the HTTP request.

## Important patterns and preferences:
- Use `requests` library for HTTP requests in Python.
- Use `json` library for JSON serialization.

## Learnings and project insights:
- Clarified the distinction between global Cline rules and project-specific memory bank files.
- Confirmed the need for explicit ACT MODE for file system operations.
