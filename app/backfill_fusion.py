"""One-time/repeatable command to create global sessions from stored local data."""

from app.database import SessionLocal, initialize_database
from app.ingest import backfill_global_sessions


def main() -> None:
    initialize_database()
    with SessionLocal.begin() as session:
        created = backfill_global_sessions(session)
    print(f"fusion backfill completed: {created} local session(s) linked")


if __name__ == "__main__":
    main()
