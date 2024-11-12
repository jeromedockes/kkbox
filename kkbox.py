import datetime

import ibis
from utils import db_path

import skrub

# %%

con = ibis.connect(f"duckdb://{db_path}", read_only=True)
transactions = con.table("transactions")
# transactions = skrub.var("transactions", transactions)

# %%
t_view = transactions.select(["msno", "transaction_date"])
transactions = transactions.asof_join(
    t_view,
    transactions.transaction_date < t_view.transaction_date,
    transactions.msno == t_view.msno,
    rname="next_{name}",
).drop("next_msno")

# %%
transactions = transactions.mutate(
    days_wo_membership=transactions.next_transaction_date.delta(
        transactions.membership_expire_date, "day"
    )
)
# TODO: many transactions have their membership_expire_date set to unix epoch
# (1970-01-01) in the original dataset csv
transactions = transactions.mutate(
    churn=(transactions.days_wo_membership > 30)
    | transactions.next_transaction_date.isnull()
)

# %%
transactions = transactions.order_by(("msno", "transaction_date"))

# %%
ibis.to_sql(transactions)

# %%
skrub.TableReport(transactions.head(20_000).to_pandas()).open()

# %%
# transactions.skb.draw_graph().open()
