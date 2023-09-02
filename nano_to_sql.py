import argparse
import datetime
import decimal
import enum
import multiprocessing
from time import sleep

import nano
import sqlalchemy
from retry import retry
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

decimal.getcontext().prec = 100

ZERO_ACCOUNT = "nano_1111111111111111111111111111111111111111111111111111hifc8npp"  # account with public key = 0

parser = argparse.ArgumentParser(description="Script for processing Nano transactions")

parser.add_argument("connection", type=str, help="Database connection string")
parser.add_argument("--rpc-node", type=str, default="http://localhost:7076", help="RPC node address")
parser.add_argument("--parallelism", type=int, default=64, help="Number of parallel workers")
parser.add_argument("--awaiting-max", type=int, default=4096, help="Max awaiting results")
parser.add_argument("--batch-size", type=int, default=4096, help="Batch size")
parser.add_argument("--history-batch-size", type=int, default=4096, help="Account history batch size")
parser.add_argument("--rpc-timeout", type=int, default=15, help="RPC timeout")
parser.add_argument("--start-account", type=str, default=ZERO_ACCOUNT, help="Starting account address")
parser.add_argument("--sql-echo", action="store_true", help="Enable SQL echo")
parser.add_argument("--bottom-up", action="store_true", help="Enable bottom-up processing")

args = parser.parse_args()
# print(args)

CONNECTION_STR = args.connection
RPC_NODE = args.rpc_node
BATCH_SIZE = args.batch_size
HISTORY_BATCH_SIZE = args.history_batch_size
PARALLELISM = args.parallelism
AWAITING_MAX = args.awaiting_max
SQL_ECHO = args.sql_echo
BOTTOM_UP = args.bottom_up
RPC_TIMEOUT = args.rpc_timeout
START_ACCOUNT = args.start_account


class Base(DeclarativeBase):
    pass


class TxType(enum.Enum):
    send = 1
    receive = 2
    other = 3


class Transaction(Base):
    __tablename__ = "transactions"

    hash: Mapped[str] = mapped_column(sqlalchemy.String(64), primary_key=True)  # hash
    account: Mapped[str] = mapped_column(sqlalchemy.String(65))  # account
    tx_type: Mapped[TxType]
    amount: Mapped[decimal.Decimal]
    link: Mapped[str] = mapped_column(sqlalchemy.String(65))  # account
    tstamp: Mapped[datetime.datetime]


engine = sqlalchemy.create_engine(CONNECTION_STR, echo=SQL_ECHO)


rpc = nano.rpc.Client(RPC_NODE, timeout=RPC_TIMEOUT)
print(rpc.version())


@retry(tries=60, delay=1)
def get_ledger(account):
    return rpc.ledger(account=account, count=BATCH_SIZE)


@retry(tries=60, delay=1)
def get_account_history(account, start=None):
    payload = {"account": account, "count": HISTORY_BATCH_SIZE, "reverse": BOTTOM_UP}
    if start:
        payload["head"] = start

    resp = rpc.call("account_history", payload)

    history = resp.get("history") or []
    previous = resp.get("next" if BOTTOM_UP else "previous") or None

    return history, previous


def process_account(account):
    cnt = 0

    history, previous = get_account_history(account)
    while len(history) > 0:
        # print(account, ":", len(history))

        should_continue, added_cnt = process_history(account, history)
        cnt += added_cnt

        if previous and should_continue:
            history, previous = get_account_history(account, previous)
        else:
            break

    return cnt


MNANO = decimal.Decimal("1000000000000000000000000000000")


def process_history(source_account, history):
    cnt = 0

    with Session(engine) as session:
        for block in history:
            # print(source_account, ":", block)

            if not block["confirmed"] == "true":
                continue  # skip unconfirmed

            hash = block["hash"]

            if session.query(Transaction).get(hash) is not None:
                return False, cnt  # skip already processed, do not continue

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
                tstamp=timestamp,
            )

            session.add(transaction)
            cnt += 1

        session.commit()

    return True, cnt  # continue


class WorkStealingManager:
    def __init__(self, worker_fn, pool_size):
        self.worker_fn = worker_fn
        self.pool = multiprocessing.get_context("spawn").Pool(pool_size)
        self.waiting_results = []

    def queue_work(self, work_items):
        for item in work_items:
            async_result = self.pool.apply_async(self.worker_fn, (item,))
            self.waiting_results.append(async_result)

    def collect_results(self):
        results = []
        for result in self.waiting_results:
            if result.ready():
                self.waiting_results.remove(result)
                results.append(result.get())
        return results

    def pending_count(self):
        return len(self.waiting_results)


def main():
    Base.metadata.create_all(engine)

    pool = WorkStealingManager(process_account, PARALLELISM)

    last_account = START_ACCOUNT

    total_accounts = 0
    total_blocks = 0

    while True:
        # print("requesting:", last_account)
        data = get_ledger(last_account)

        accounts = []
        for account, info in sorted(data.items()):
            if account == last_account:
                continue  # prevent processing same account twice
            last_account = account
            
            accounts.append(account)

            if info["block_count"] > HISTORY_BATCH_SIZE:
                print("WARNING: account", account, f"has more than {HISTORY_BATCH_SIZE} blocks:", info["block_count"])

        if len(accounts) == 0:
            print("WARNING: no more accounts found")
            break

        pool.queue_work(accounts)

        while True:
            results = pool.collect_results()
            total_blocks += sum(results)
            total_accounts += len(results)

            print(
                "Processed:",
                total_accounts,
                "accounts,",
                total_blocks,
                "blocks,",
                "last account:",
                last_account,
                f"(awaiting results: {pool.pending_count()})",
            )

            if pool.pending_count() > AWAITING_MAX:
                # print("NOTICE: awaiting results:", len(waiting_results))
                sleep(1)
            else:
                break

        pass


if __name__ == "__main__":
    main()

    print("Done")
