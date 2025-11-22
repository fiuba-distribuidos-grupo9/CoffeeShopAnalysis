import logging

from controllers.filters.filter_transactions_by_final_amount.filter_transactions_by_final_amount import (
    FilterTransactionsByFinalAmount,
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
            "MIN_FINAL_AMOUNT",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "queue_name_prefix": constants.FILTERED_TRN_BY_YEAR__HOUR_1X_QUEUE_PREFIX,
        "prev_controllers_amount": int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    }
    producers_config = {
        "queue_name_prefix": constants.FILTERED_TRN_BY_YEAR__HOUR__FINAL_AMOUNT_QUEUE_PREFIX,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = FilterTransactionsByFinalAmount(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumers_config=consumers_config,
        producers_config=producers_config,
        min_final_amount=float(config_params["MIN_FINAL_AMOUNT"]),
    )
    controller.run()


if __name__ == "__main__":
    main()
