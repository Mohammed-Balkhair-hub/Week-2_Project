import sys
from pathlib import Path


# Make `src/` importable when running as a script
ROOT = Path(__file__).resolve().parents[1]# init root path as the parent if this path dir
SRC = ROOT / "src" 
sys.path.insert(0, str(SRC))# move python env path to my package


from data_workflow.config import make_paths
from data_workflow.io import  read_orders_csv,read_users_csv,write_parquet
from data_workflow.transformers import enforce_schema
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

log = logging.getLogger(__name__)


def main() -> None:
    
    path=make_paths(ROOT)# intilize the config class which has all data dir paths
    log.info("Paths made successfully... processing will started soon ....\n")

    # 1-Loads orders.csv and applies schema
    log.info("###########################  Start loading orders Data  ###########################")# flag of starting the process of task 1
    orders=enforce_schema(read_orders_csv(path.raw/"orders.csv"))# read csv and enforce data type schema
    log.info("orders data loaded and schema applied ... Done\n")# flag of finishing the process of task 1

    # 2-Loads users.csv
    log.info("###########################  Start loading users Data  ###########################")# flag of starting the process of task 2
    users=read_users_csv(path.raw/"users.csv")# read the csv file for users
    log.info("users data loaded ... Done\n")# flag of finishing the process of task 2

    # 3-Writes orders.parquet
    log.info("exporting orders to parquet ... ")# flag of starting the process of task 3
    write_parquet(orders,path.processed/"orders.parquet")# export the df to data/processed/orders.parquet
    log.info(f"orders info have been exported to {path.processed/"orders.parquet"} ...Done\n")# flag of finishing the process of task 3

    # 4-Writes users.parquet
    log.info("exporting users to parquet ... ")# flag of starting the process of task 4
    write_parquet(users,path.processed/"users.parquet")# export the df to data/processed/users.parquet
    log.info(f"users info have been exported to {path.processed/"users.parquet"} ...Done\n")# flag of finishing the process of task 4

    log.info(f"Summary: number of rows for orders is {len(orders)} & for users {len(users)}")
    log.info(f"Processed output can be found in {path.processed}\n")

    return None
    




if __name__ == "__main__":
    main()


