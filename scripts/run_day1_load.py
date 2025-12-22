import sys
from pathlib import Path


# Make `src/` importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))


from data_workflow.config import make_paths
from data_workflow.io import  read_orders_csv,read_users_csv,write_parquet
from data_workflow.transformers import enforce_schema
import logging
log=logging.getLogger(__name__)



log = logging.getLogger(__name__)

def main() -> None:
    
    path=make_paths(ROOT)
    orders=enforce_schema(read_orders_csv(path.raw/"orders.csv"))
    log.info(" schema of of orders data is applied")

    users=read_users_csv(path.raw/"users.csv")
    log.info(" read user data applied data is applied")

    write_parquet(orders,path.processed/"orders.parquet")
    log.info(" orders info have been exported to parquet")

    write_parquet(users,path.processed/"users.parquet")
    log.info(" users info have been exported to parquet")

    log.info(f"the number of rows for order is {len(orders)} & for users {len(users)}")

    log.info(f" you can find processed output in {path.processed}")

    log.info("everything is Done!")
    




if __name__ == "__main__":
    main()


