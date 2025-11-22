import logging

from controllers.cleaners.stores_cleaner.stores_cleaner import StoresCleaner
from shared import constants, initializer


def main():
    config_params = initializer.init_config(
        [
            "LOGGING_LEVEL",
            "CONTROLLER_ID",
            "RABBITMQ_HOST",
            "NEXT_CONTROLLERS_AMOUNT",  # @TODO: add another env variable
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "queue_name_prefix": constants.DIRTY_STR_QUEUE_PREFIX,
    }
    producers_config = {
        "queue_name_prefix_1": constants.CLEANED_STR_3X_QUEUE_PREFIX,
        "queue_name_prefix_2": constants.CLEANED_STR_4X_QUEUE_PREFIX,
        "next_controllers_amount_1": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
        "next_controllers_amount_2": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    cleaner = StoresCleaner(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumers_config=consumers_config,
        producers_config=producers_config,
    )
    cleaner.run()


if __name__ == "__main__":
    main()
