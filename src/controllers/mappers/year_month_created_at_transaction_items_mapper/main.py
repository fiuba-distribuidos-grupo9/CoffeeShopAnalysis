import logging

from controllers.mappers.year_month_created_at_transaction_items_mapper.year_month_created_at_transaction_items_mapper import (
    YearMonthCreatedAtTransactionItemsMapper,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "HEALTH_LISTEN_PORT",
            "PREV_CONTROLLERS_AMOUNT",
            "NEXT_CONTROLLERS_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "queue_name_prefix": constants.FILTERED_TIT_BY_YEAR_QUEUE_PREFIX,
        "prev_controllers_amount": int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    }
    producers_config = {
        "queue_name_prefix_1": constants.MAPPED_YEAR_MONTH_TIT_21_QUEUE_PREFIX,
        "queue_name_prefix_2": constants.MAPPED_YEAR_MONTH_TIT_22_QUEUE_PREFIX,
        "next_controllers_amount_1": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
        "next_controllers_amount_2": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = YearMonthCreatedAtTransactionItemsMapper(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        health_listen_port=int(config_params["HEALTH_LISTEN_PORT"]),
        consumers_config=consumers_config,
        producers_config=producers_config,
    )
    controller.run()


if __name__ == "__main__":
    main()
