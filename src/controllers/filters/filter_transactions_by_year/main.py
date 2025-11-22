import logging

from controllers.filters.filter_transactions_by_year.filter_transactions_by_year import (
    FilterTransactionsByYear,
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
        "queue_name_prefix": constants.CLEANED_TRN_1X_QUEUE_PREFIX,
        "prev_controllers_amount": int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    }
    producers_config = {
        "queue_name_prefix": constants.FILTERED_TRN_BY_YEAR_QUEUE_PREFIX,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = FilterTransactionsByYear(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumers_config=consumers_config,
        producers_config=producers_config,
        years_to_keep=years_to_keep,
    )
    controller.run()


if __name__ == "__main__":
    main()
