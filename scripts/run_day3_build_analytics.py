import sys
from pathlib import Path


# Make `src/` importable when running as a script
ROOT = Path(__file__).resolve().parents[1]# init root path as the parent if this path dir
SRC = ROOT / "src" 
sys.path.insert(0, str(SRC))# move python env path to my package


from data_workflow.config import make_paths
from data_workflow.io import  read_orders_csv,read_users_csv,write_parquet,write_csv,read_parquet
from data_workflow.transformers import enforce_schema,missingness_report,normalize_text,add_missing_flags,parse_datetime,add_time_parts,iqr_bounds,winsorize,add_outlier_flag
from data_workflow.quality import require_columns,assert_non_empty,assert_unique_key
from data_workflow.joins import safe_left_join
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

log = logging.getLogger(__name__)



def main() -> None:

    path=make_paths(ROOT)# intilize the config class which has all data dir paths
    log.info("Paths made successfully... processing will started soon ....\n")

    # 1-Loads orders_clean.parquet and users.parquet
    log.info("###########################  Start loading orders and users Data  ###########################")# flag of starting the process of task 1
    orders=read_parquet(path.processed/"orders_cleaned.parquet")# read the parquet file for orders
    users=read_parquet(path.processed/"users.parquet")# read the parquet file for users
    log.info("data loaded ... Done\n")# flag of finishing the process of task 1






    #2-Validates inputs (columns, uniqueness)
    log.info("check columns requriment ... ")# flag of starting the process of task 2
    require_columns(orders, ["order_id","user_id","amount","quantity","created_at","status"])# cheak if required columns exist in orders table  
    orders=enforce_schema(orders)# enofrce data type schema into orders table 
    require_columns(users, ["user_id","country","signup_date"])# cheak if required columns exist in users table 
    users['user_id']=users["user_id"].astype("string")
    assert_unique_key(users, "user_id")# validation check that user_id is a unique key
    log.info("check columns requriment ... Done\n")# flag of finishing the process of task 2






    log.info("parse date columns ... ")# flag of starting the process of task 3
    # 3- parse created at in orders + add time parts
    orders=parse_datetime(orders,"created_at")# will convert text column --> date column
    orders=add_time_parts(orders,"created_at")# will add new columns contain date's part for eg. year will be in a column and hour will also be in a seprated one
    log.info("parse date columns ... Done\n")# flag of finishing the process of task 3




    log.info("merging Data frames ... ")# flag of starting the process of task 4
    #4- Joins orders → users (validate="many_to_one")
    df_merged=safe_left_join(orders,users,on="user_id",validate="many_to_one",)# it will do left join so the left table will preserve all it's rows 
    assert len(df_merged) == len(orders) # check row count changed  to detect join explosion

    log.info("merging Data frames ... Done\n")# flag of finishing the process of task 4






    log.info("calculating percentiles and removing outliers ...")# flag of starting the process of task 5
    # 5- Winsorizes amount and adds outlier flag
    df_merged['amount']=winsorize(df_merged['amount'],lo=0.01,hi=0.99)# clip extream outliers
    df_merged=add_outlier_flag(df_merged,"amount",k=1.2)# flag some acceptable outliers that have not been cliped
    log.info("calculating percentiles and removing outliers ... Done\n")# flag of finishing the process of task 5




    #6- Writes analytics_table.parquet
    log.info(f"start exporting the analytics table to {path.processed/"analytics_table.parquet"}  ...")# flag of starting the process of task 6
    write_parquet(df_merged,path.processed/"analytics_table.parquet")# export the df to data/processed/analytics_table.parquet
    log.info(f"Exporting results to {path.processed/"analytics_table.parquet"} ...Done \n")# flag of finishing the process of task 6






    return None









if __name__ == "__main__":
    main()
