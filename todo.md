Todos
    Finish workflow diagrams (register.md, become_auctioneer.md)

    Scaffold src/ folder structure (shared/ + module stubs)

    Initialize Node.js project (package.json, TypeScript config, dependencies)

    Set up database migrations (shared/db/migrations/)

    Implement Auth module (login, logout, session middleware)

    Implement Users module (register, become auctioneer)

    Implement Auctions module (create, start, end — with atomic state transitions)

    Implement Items module (placeholder fields, tied to auctions)

    Design and implement real-time update strategy (WebSockets / SSE for live auction events)

    Mock payments flow
______



That's the list. A few things worth noting about the order:

Workflow diagrams first — they're already in progress and will inform the module implementations
Project init + scaffolding before any real code, so the structure is right from the start
Real-time strategy is intentionally near the end — it's the core focus but needs the auction state machine working first to have something meaningful to stream
Want to start with the workflow diagrams, or jump straight to project initialization?