import datetime
import decimal
import enum

import nano
import sqlalchemy
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

decimal.getcontext().prec = 100

CONNECTION_STR = "postgresql://postgres:123456@localhost:5432/postgres"
RPC_NODE = "http://localhost:7076"
LEDGER_BATCH_SIZE = 16


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


engine = sqlalchemy.create_engine(CONNECTION_STR, echo=True)
Base.metadata.create_all(engine)

rpc = nano.rpc.Client(RPC_NODE)
print(rpc.version())


def process_account(account):
    history = rpc.account_history(account=account, count=-1)
    # print(account, ":", len(history))

    process_history(account, history)

    return len(history)


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
            amount = decimal.Decimal(block["amount"]) / decimal.Decimal(
                "1000000000000000000000000000000"
            )

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


# last_account = "nano_1111111111111111111111111111111111111111111111111111hifc8npp"  # account with public key = 0
last_account = "nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est"

while True:
    print("requesting:", last_account)
    data = rpc.ledger(account=last_account, count=LEDGER_BATCH_SIZE)

    for account, info in sorted(data.items()):
        if account == last_account:
            continue  # prevent processing same account twice
        last_account = account

        block_cnt = process_account(account)

    pass
