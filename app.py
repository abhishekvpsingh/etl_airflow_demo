import pandas as pd
from loguru import logger
from sys import argv
from read import read_table
from write import write_to_file


def initlog():
    logger.add("etl_logs.info",
               rotation="1 MB",
               retention="10 days",
               level="INFO"
               )


def main():
    initlog()
    a_table = argv[1]
    logger.info(f'Reading data for {a_table.upper()}')
    df = read_table(a_table)
    logger.info(f'Writing data for {a_table.upper()} into tmp fpr processing')
    write_to_file('/tmp', table_name=a_table, df=df)


if __name__ == '__main__':
    main()
