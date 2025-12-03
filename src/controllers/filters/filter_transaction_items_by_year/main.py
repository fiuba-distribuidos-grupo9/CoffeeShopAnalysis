import logging

from controllers.filters.filter_transaction_items_by_year.filter_transaction_items_by_year import (
    FilterTransactionItemsByYear,
)
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "PREV_CONTROLLERS_AMOUNT",
            "NEXT_CONTROLLERS_AMOUNT",
            "YEARS_TO_KEEP",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    year_list = config_params["YEARS_TO_KEEP"].split(",")
    years_to_keep = [int(year) for year in year_list]

    consumers_config = {
        "queue_name_prefix": constants.CLEANED_TIT_2X_QUEUE_PREFIX,
        "prev_controllers_amount": int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    }
    producers_config = {
        "queue_name_prefix": constants.FILTERED_TIT_BY_YEAR_QUEUE_PREFIX,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = FilterTransactionItemsByYear(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumers_config=consumers_config,
        producers_config=producers_config,
        years_to_keep=years_to_keep,
    )
    controller.run()


if __name__ == "__main__":
    main()
