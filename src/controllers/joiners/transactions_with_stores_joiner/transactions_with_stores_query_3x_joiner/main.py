import logging

from controllers.joiners.transactions_with_stores_joiner.shared.transactions_with_stores_joiner import (
    TransactionsWithStoresJoiner,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "HEALTH_LISTEN_PORT",
            "BASE_DATA_PREV_CONTROLLERS_AMOUNT",
            "STREAM_DATA_PREV_CONTROLLERS_AMOUNT",
            "NEXT_CONTROLLERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "base_data_queue_name_prefix": constants.CLEANED_STR_3X_QUEUE_PREFIX,
        "base_data_prev_controllers_amount": int(
            config_params["BASE_DATA_PREV_CONTROLLERS_AMOUNT"]
        ),
        "stream_data_queue_name_prefix": constants.SUM_TRN_TPV_BY_STORE_QUEUE_PREFIX,
        "stream_data_prev_controllers_amount": int(
            config_params["STREAM_DATA_PREV_CONTROLLERS_AMOUNT"]
        ),
    }
    producers_config = {
        "queue_name_prefix": constants.TPV_BY_HALF_YEAR_CREATED_AT__STORE_NAME_QUEUE_PREFIX,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = TransactionsWithStoresJoiner(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        health_listen_port=int(config_params["HEALTH_LISTEN_PORT"]),
        consumers_config=consumers_config,
        producers_config=producers_config,
    )
    controller.run()


if __name__ == "__main__":
    main()
