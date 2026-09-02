# Auction Service

Independently buildable and testable foundation for the auction service.

Its initial responsibility is auction lifecycle management and synchronous bid acceptance. Auctions and bids remain in one service initially because their correctness rules require a clear transactional boundary.

The current entry point is intentionally minimal. Domain behavior and runtime
configuration will be added in later milestones.
