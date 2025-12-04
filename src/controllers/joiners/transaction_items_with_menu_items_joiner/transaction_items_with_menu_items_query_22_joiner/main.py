import logging

from controllers.joiners.transaction_items_with_menu_items_joiner.shared.transaction_items_with_menu_items_joiner import (
    TransactionItemsWithMenuItemsJoiner,
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
        "base_data_queue_name_prefix": constants.CLEANED_MIT_22_QUEUE_PREFIX,
        "base_data_prev_controllers_amount": int(
            config_params["BASE_DATA_PREV_CONTROLLERS_AMOUNT"]
        ),
        "stream_data_queue_name_prefix": constants.SORTED_DESC_PROFIT_SUM_BY_YEAR_MONTH__ITEM_ID_QUEUE_PREFIX,
        "stream_data_prev_controllers_amount": int(
            config_params["STREAM_DATA_PREV_CONTROLLERS_AMOUNT"]
        ),
    }
    producers_config = {
        "queue_name_prefix": constants.SORTED_DESC_PROFIT_SUM_BY_YEAR_MONTH__ITEM_NAME_QUEUE_PREFIX,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = TransactionItemsWithMenuItemsJoiner(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        health_listen_port=int(config_params["HEALTH_LISTEN_PORT"]),
        consumers_config=consumers_config,
        producers_config=producers_config,
    )
    controller.run()


if __name__ == "__main__":
    main()
