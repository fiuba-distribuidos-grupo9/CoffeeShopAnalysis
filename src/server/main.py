import logging

from server.server import Server
from shared import constants, initializer


def _build_cleaners_data(config_params: dict) -> dict:
    menu_items_workers_amount = int(config_params["MENU_ITEMS_CLN_AMOUNT"])
    stores_workers_amount = int(config_params["STORES_CLN_AMOUNT"])
    transaction_items_workers_amount = config_params["TRANSACTION_ITEMS_CLN_AMOUNT"]
    transactions_workers_amount = int(config_params["TRANSACTIONS_CLN_AMOUNT"])
    users_workers_amount = int(config_params["USERS_CLN_AMOUNT"])

    return {
        constants.MENU_ITEMS: {
            constants.QUEUE_PREFIX: constants.DIRTY_MIT_QUEUE_PREFIX,
            constants.WORKERS_AMOUNT: int(menu_items_workers_amount),
        },
        constants.STORES: {
            constants.QUEUE_PREFIX: constants.DIRTY_STR_QUEUE_PREFIX,
            constants.WORKERS_AMOUNT: int(stores_workers_amount),
        },
        constants.TRANSACTION_ITEMS: {
            constants.QUEUE_PREFIX: constants.DIRTY_TIT_QUEUE_PREFIX,
            constants.WORKERS_AMOUNT: int(transaction_items_workers_amount),
        },
        constants.TRANSACTIONS: {
            constants.QUEUE_PREFIX: constants.DIRTY_TRN_QUEUE_PREFIX,
            constants.WORKERS_AMOUNT: int(transactions_workers_amount),
        },
        constants.USERS: {
            constants.QUEUE_PREFIX: constants.DIRTY_USR_QUEUE_PREFIX,
            constants.WORKERS_AMOUNT: int(users_workers_amount),
        },
    }


def _build_output_builders_data(config_params: dict) -> dict:
    q1x_worker_amount = config_params["Q1X_OB_AMOUNT"]
    q21_worker_amount = config_params["Q21_OB_AMOUNT"]
    q22_worker_amount = config_params["Q22_OB_AMOUNT"]
    q3x_worker_amount = config_params["Q3X_OB_AMOUNT"]
    q4x_worker_amount = config_params["Q4X_OB_AMOUNT"]

    return {
        constants.QUERY_RESULT_1X: {
            constants.QUEUE_PREFIX: constants.QRS_QUEUE_PREFIX,
            constants.WORKERS_AMOUNT: int(q1x_worker_amount),
        },
        constants.QUERY_RESULT_21: {
            constants.QUEUE_PREFIX: constants.QRS_QUEUE_PREFIX,
            constants.WORKERS_AMOUNT: int(q21_worker_amount),
        },
        constants.QUERY_RESULT_22: {
            constants.QUEUE_PREFIX: constants.QRS_QUEUE_PREFIX,
            constants.WORKERS_AMOUNT: int(q22_worker_amount),
        },
        constants.QUERY_RESULT_3X: {
            constants.QUEUE_PREFIX: constants.QRS_QUEUE_PREFIX,
            constants.WORKERS_AMOUNT: int(q3x_worker_amount),
        },
        constants.QUERY_RESULT_4X: {
            constants.QUEUE_PREFIX: constants.QRS_QUEUE_PREFIX,
            constants.WORKERS_AMOUNT: int(q4x_worker_amount),
        },
    }


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "SERVER_PORT",
            "SERVER_LISTEN_BACKLOG",
            "RABBITMQ_HOST",
            "HEALTH_LISTEN_PORT",
            "MENU_ITEMS_CLN_AMOUNT",
            "STORES_CLN_AMOUNT",
            "TRANSACTION_ITEMS_CLN_AMOUNT",
            "TRANSACTIONS_CLN_AMOUNT",
            "USERS_CLN_AMOUNT",
            "Q1X_OB_AMOUNT",
            "Q21_OB_AMOUNT",
            "Q22_OB_AMOUNT",
            "Q3X_OB_AMOUNT",
            "Q4X_OB_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    server = Server(
        port=int(config_params["SERVER_PORT"]),
        listen_backlog=int(config_params["SERVER_LISTEN_BACKLOG"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        health_listen_port=int(config_params["HEALTH_LISTEN_PORT"]),
        cleaners_data=_build_cleaners_data(config_params),
        output_builders_data=_build_output_builders_data(config_params),
    )
    server.run()


if __name__ == "__main__":
    main()
