import pandas as pd
from loguru import logger
import os

logger.add("etl_logs.info",
           rotation="1 MB",
           retention="10 days",
           level="INFO"
           )


def write_to_file(base_dir, table_name, df):
    file_path = f'{base_dir}/{table_name}/data-00000'
    if not os.path.exists(f'{base_dir}/{table_name}'):
        os.makedirs(f'{base_dir}/{table_name}')
    df.to_csv(file_path, index=False)
    logger.info(f'{table_name.upper()} table is written.')
