import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))


from data_workflow.etl import run_etl
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

log = logging.getLogger(__name__)


def main() -> None:
    log.info("starting ETL pipeline...")
    run_etl(ROOT)
    log.info("ETL pipeline completed successfully")
    return None


if __name__ == "__main__":
    main()

