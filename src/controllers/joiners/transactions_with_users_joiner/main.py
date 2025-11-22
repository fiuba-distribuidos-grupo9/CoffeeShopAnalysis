import logging

from controllers.joiners.transactions_with_users_joiner.transactions_with_users_joiner import (
    TransactionsWithUsersJoiner,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "BASE_DATA_PREV_CONTROLLERS_AMOUNT",
            "STREAM_DATA_PREV_CONTROLLERS_AMOUNT",
            "NEXT_CONTROLLERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "base_data_queue_name_prefix": constants.CLEANED_USR_4X_QUEUE_PREFIX,
        "base_data_prev_controllers_amount": int(
            config_params["BASE_DATA_PREV_CONTROLLERS_AMOUNT"]
        ),
        "stream_data_queue_name_prefix": constants.SORTED_DESC_BY_STORE_ID__PURCHASES_QTY_WITH_USER_ID,
        "stream_data_prev_controllers_amount": int(
            config_params["STREAM_DATA_PREV_CONTROLLERS_AMOUNT"]
        ),
    }
    producers_config = {
        "queue_name_prefix": constants.SORTED_DESC_BY_STORE_ID__PURCHASES_QTY_WITH_USER_BITHDATE,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = TransactionsWithUsersJoiner(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumers_config=consumers_config,
        producers_config=producers_config,
    )
    controller.run()


if __name__ == "__main__":
    main()
