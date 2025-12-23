import sys
from pathlib import Path


# Make `src/` importable when running as a script
ROOT = Path(__file__).resolve().parents[1]# init root path as the parent if this path dir
SRC = ROOT / "src" 
sys.path.insert(0, str(SRC))# move python env path to my package


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
    
    path=make_paths(ROOT)# intilize the config class which has all data dir paths
    log.info("Paths made successfully... processing will started soon ....\n")

    # 1-Loads and validates orders data
    log.info("###########################  Start Orders Data Processing  ###########################")# flag of starting the process of task 1
    orders=read_orders_csv(path.raw/"orders.csv")# read the csv file for orders
    log.info(f"data has been loaded from {path.raw/"orders.csv"} ...Done ")# flag of finishing loading
    require_columns(orders, ["order_id","user_id","amount","quantity","created_at","status"])# cheak if required columns exist in orders table
    log.info("columns requirment check ... Done")# flag of finishing column check

    assert_non_empty(orders, "orders")# validation check that orders table is not empty
    log.info("non empty check ... Done")# flag of finishing non empty check
    orders=enforce_schema(orders)# enforce data type schema into orders table
    log.info("schema applied ... Done\n")# flag of finishing schema enforcement

    # 2-Generates missingness report
    log.info("generating missingness report ... ")# flag of starting the process of task 2
    orders_report=missingness_report(orders)# calculate missing values statistics
    write_csv(orders_report,ROOT/"reports"/"missings_report.csv")# export the report to csv
    log.info(f"orders missing report has been written in {ROOT/"reports"/"missings_report.csv"} ...Done\n")# flag of finishing the process of task 2

    # 3-Cleans orders data (normalize text and add missing flags)
    log.info("cleaning orders data ... ")# flag of starting the process of task 3
    status_clean=normalize_text(orders["status"])# normalize status text (trim, lowercase, collapse whitespace)
    orders_clean = (
        orders.assign(status_clean=status_clean)# add normalized status column
              .pipe(add_missing_flags, cols=["amount", "quantity"])# add boolean flags for missing values
    )
    log.info("orders data has been cleaned successfully ...Done\n")# flag of finishing the process of task 3

    # 4-Writes cleaned orders to parquet
    log.info("exporting cleaned orders to parquet ... ")# flag of starting the process of task 4
    write_parquet(orders_clean,path.processed/"orders_cleaned.parquet")# export the df to data/processed/orders_cleaned.parquet
    log.info(f"orders cleaned data has been written in {path.processed/"orders_cleaned.parquet"} ...Done\n")# flag of finishing the process of task 4

    log.info("##################################################################################\n")

    # 5-Loads and validates users data
    log.info("###########################  Start Users Data Processing  ###########################")# flag of starting the process of task 5
    users=read_users_csv(path.raw/"users.csv")# read the csv file for users
    log.info(f"data has been loaded from {path.raw/"users.csv"} ...Done ")# flag of finishing loading
    require_columns(users, ["user_id","country","signup_date"])# cheak if required columns exist in users table
    log.info("columns requirment check ... Done")# flag of finishing column check

    assert_non_empty(users, "users")# validation check that users table is not empty
    log.info("non empty check ... Done\n")# flag of finishing non empty check

    # 6-Writes users to parquet
    log.info("exporting users to parquet ... ")# flag of starting the process of task 6
    write_parquet(users,path.processed/"users.parquet")# export the df to data/processed/users.parquet
    log.info(f"users data has been written in {path.processed/"users.parquet"} ...Done\n")# flag of finishing the process of task 6

    log.info("##################################################################################")
    log.info("Script ran perfectly\n")

    return None

    




if __name__ == "__main__":
    main()


