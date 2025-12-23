import sys
from pathlib import Path


# Make `src/` importable when running as a script
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))


from data_workflow.config import make_paths
from data_workflow.io import  read_orders_csv,read_users_csv,write_parquet,write_csv
from data_workflow.transformers import enforce_schema,missingness_report,normalize_text,add_missing_flags
from data_workflow.quality import require_columns,assert_non_empty
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

log = logging.getLogger(__name__)


def main() -> None:
    
    path=make_paths(ROOT)
    log.info("Paths made successfully... processing will started soon ....\n")
    log.info("###########################  Start Orders Data Processing  ###########################")
    orders=read_orders_csv(path.raw/"orders.csv")
    log.info(f"1- data has been loaded from {path.raw/"orders.csv"} ...Done ")
    require_columns(orders, ["order_id","user_id","amount","quantity","created_at","status"])
    log.info(f"2- columns requirment check ... Done")

    assert_non_empty(orders, "orders")
    log.info("3- non empty cheack ... Done")
    orders=enforce_schema(orders)
    log.info("4- schema applied ... Done")


    orders_report=missingness_report(orders)
    write_csv(orders_report,ROOT/"reports"/"missings_report.csv")
    log.info(f"5- orders missing report has been written in {ROOT/"reports"/"missings_report.csv"}")

    status_clean=normalize_text(orders["status"])
    log.info(f"6- status text normalizing  ... Done")
    orders_clean = (
        orders.assign(status_clean=status_clean)
              .pipe(add_missing_flags, cols=["amount", "quantity"])
    )

    log.info("@@@@ orders data has been cleaned successfully")

    write_parquet(orders_clean,path.processed/"orders_cleaned.parquet")
    log.info(f" orders cleaned data has been written in {path.processed/"orders_cleaned.parquet"}")


    log.info("##################################################################################")


    log.info("###########################  Start Users Data Processing  ###########################")

    users=read_users_csv(path.raw/"users.csv")
    log.info(f"1- data has been loaded from {path.raw/"users.csv"} ...Done ")
    require_columns(users, ["user_id","country","signup_date"])
    log.info(f"2- columns requirment check ... Done")

    assert_non_empty(users, "users")
    log.info("3- non empty cheack ... Done")

    write_parquet(users,path.processed/"users.parquet")
    log.info(f"4-  users cleaned data has been written in {path.processed/"users.parquet"}")
    log.info("##################################################################################")
    log.info("############# Script ran perfectly")

    




if __name__ == "__main__":
    main()


