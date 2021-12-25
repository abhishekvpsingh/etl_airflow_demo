import pandas as pd
from loguru import logger


logger.add("etl_logs.info",
           rotation="1 MB",
           retention="10 days",
           level="INFO"
           )


def get_table_col_name(table_name):
    table_col_name = list()
    table_col = {
        'orders': ['order_id', 'order_date', 'order_customer_id', 'order_status'],
        'customers': ['customer_id', 'customer_fname', 'customer_lname', 'customer_email',
                      'customer_password', 'customer_street', 'customer_city',
                      'customer_state', 'customer_zipcode']
    }

    if table_name == 'orders':
        table_col_name = table_col['orders']
    elif table_name == 'customers':
        table_col_name = table_col['customers']
    return table_col_name


def get_table_file_path(table_name):
    table_path = ''
    if table_name == 'orders':
        table_path = '/home/abhi/etl_airflow_demo/orders/part-00000'
    elif table_name == 'customers':
        table_path = '/home/abhi/etl_airflow_demo/customers/part-00000'
    return table_path


def read_table(table_name):
    table_path = get_table_file_path(table_name)
    table_col_name = get_table_col_name(table_name)
    df = pd.read_csv(table_path, header=None,
                     delimiter=',', names=table_col_name
                     )
    logger.info(f'{table_name.upper()} table is read.')
    return df


if __name__ == '__main__':
    read_table('orders')
