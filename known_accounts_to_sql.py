import argparse
import json

import sqlalchemy
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Base(DeclarativeBase):
    pass


class KnownAccount(Base):
    __tablename__ = "known_accounts"

    account: Mapped[str] = mapped_column(sqlalchemy.String(65), primary_key=True)
    name: Mapped[str] = mapped_column(sqlalchemy.String)


def load_accounts_from_json(filename):
    with open(filename, "r") as file:
        return json.load(file)


def main(args):
    engine = sqlalchemy.create_engine(args.connection_str, echo=args.sql_echo)

    Base.metadata.create_all(engine)

    accounts = load_accounts_from_json(args.file_path)

    with Session(engine) as session:
        for info in accounts:
            account = info["account"]
            alias = info["alias"]

            if account is None or len(account) == 0:
                continue
            if alias is None or len(alias) == 0:
                continue

            existing = session.query(KnownAccount).get(account)
            if existing is not None:
                print(f"Account {account} ({alias}) already exists, updating...")
                session.delete(existing)

            print(f"Adding account {account} ({alias})")

            known_account = KnownAccount(account=account, name=alias)
            session.add(known_account)

        session.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add known accounts from a JSON file to a database.")

    parser.add_argument("connection_str", type=str, help="SQLAlchemy connection string")
    parser.add_argument("file_path", type=str, help="Path to the knownAccounts.json file")
    parser.add_argument("--sql_echo", type=bool, default=False, help="Print SQL queries (default: False)")

    args = parser.parse_args()
    main(args)
