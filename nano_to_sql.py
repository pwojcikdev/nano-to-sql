import datetime
import decimal
import enum
import multiprocessing
from time import sleep

import nano
import requests
import sqlalchemy
import urllib3
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from retry import retry

decimal.getcontext().prec = 100

CONNECTION_STR = "postgresql://postgres:123456@localhost:5432/postgres"
RPC_NODE = "http://localhost:7076"
BATCH_SIZE = 1024
HISTORY_BATCH_SIZE = 4096
POOL_SIZE = 128
AWAITING_MAX = 1024 * 16
SQL_ECHO = False

START_ACCOUNT = "nano_1111111111111111111111111111111111111111111111111111hifc8npp"  # account with public key = 0
# START_ACCOUNT = "nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est"


class Base(DeclarativeBase):
    pass


class TxType(enum.Enum):
    send = 1
    receive = 2
    other = 3


class Transaction(Base):
    __tablename__ = "transaction"

    hash: Mapped[str] = mapped_column(sqlalchemy.String(64), primary_key=True)  # hash
    account: Mapped[str] = mapped_column(sqlalchemy.String(65))  # account
    tx_type: Mapped[TxType]
    amount: Mapped[decimal.Decimal]
    link: Mapped[str] = mapped_column(sqlalchemy.String(65))  # account
    time: Mapped[datetime.datetime]


engine = sqlalchemy.create_engine(CONNECTION_STR, echo=SQL_ECHO)


rpc = nano.rpc.Client(RPC_NODE)
print(rpc.version())


@retry(tries=100)
def get_account_history(account, start=None):
    payload = {"account": account, "count": HISTORY_BATCH_SIZE}
    if start:
        payload["head"] = start

    resp = rpc.call("account_history", payload)

    history = resp.get("history") or []
    previous = resp.get("previous") or None

    return history, previous


def process_account(account):
    cnt = 0

    history, previous = get_account_history(account)
    while len(history) > 0:
        # print(account, ":", len(history))

        process_history(account, history)
        cnt += len(history)

        if previous:
            history, previous = get_account_history(account, previous)
        else:
            break

    return cnt


MNANO = decimal.Decimal("1000000000000000000000000000000")


def process_history(source_account, history):
    with Session(engine) as session:
        for block in history:
            # print(source_account, ":", block)

            if not block["confirmed"] == "true":
                continue  # skip unconfirmed

            hash = block["hash"]
            tx_type = TxType[block["type"]]
            link = block["account"]
            timestamp = datetime.datetime.fromtimestamp(int(block["local_timestamp"]))

            # convert to the usual nano units
            amount = decimal.Decimal(block["amount"]) / MNANO

            transaction = Transaction(
                hash=hash,
                account=source_account,
                tx_type=tx_type,
                amount=amount,
                link=link,
                time=timestamp,
            )

            session.add(transaction)

        session.commit()


def main():
    Base.metadata.create_all(engine)

    pool = multiprocessing.get_context("spawn").Pool(POOL_SIZE)

    last_account = START_ACCOUNT

    total_accounts = 0
    total_blocks = 0

    waiting_results = []

    while True:
        # print("requesting:", last_account)
        data = rpc.ledger(account=last_account, count=BATCH_SIZE)

        accounts = []
        for account, info in sorted(data.items()):
            if account == last_account:
                continue  # prevent processing same account twice
            last_account = account
            accounts.append(account)

            if info["block_count"] > HISTORY_BATCH_SIZE:
                print("WARNING: account", account, f"has more than {HISTORY_BATCH_SIZE} blocks:", info["block_count"])

        for account in accounts:
            async_result = pool.apply_async(process_account, (account,))
            waiting_results.append(async_result)

        def cleanup():
            loop_total = 0
            for result in waiting_results:
                if result.ready():
                    waiting_results.remove(result)
                    loop_total += result.get()
            return loop_total

        while True:
            loop_total = cleanup()

            total_accounts += len(accounts)
            total_blocks += loop_total

            print(
                "Processed:",
                total_accounts,
                "accounts,",
                total_blocks,
                "blocks,",
                "last account:",
                last_account,
                "(awaiting results:",
                len(waiting_results),
                ")",
            )

            if len(waiting_results) > AWAITING_MAX:
                # print("NOTICE: awaiting results:", len(waiting_results))
                sleep(1)
            else:
                break

        pass


if __name__ == "__main__":
    main()
