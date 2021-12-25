import pandas as pd
from loguru import logger
import os


def join_data():
    logger.info(f'Order table is loading.')
    orders = pd.read_csv('/tmp/orders/data-00000')
    logger.info(f'Customer table is loading.')
    customers = pd.read_csv('/tmp/customers/data-00000')

    logger.info(f'Joinning the data')
    order_count_by_customer = customers.join(orders.set_index('order_customer_id'), on='customer_id', how='inner'). \
        groupby('customer_id')['customer_id'].agg(['count']).rename(columns={'count': 'order_count'})

    if not os.path.exists(f'/tmp/join_orders_and_customers'):
        os.makedirs(f'/tmp/join_orders_and_customers')
    logger.info(f'Writing the resultant data into file')
    order_count_by_customer.to_csv('/tmp/join_orders_and_customers/data-00000')


if __name__ == '__main__':
    join_data()



