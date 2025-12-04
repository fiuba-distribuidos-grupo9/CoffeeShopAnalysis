import logging

from controllers.output_builders.query_22_output_builder.query_22_output_builder import (
    Query22OutputBuilder,
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
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "queue_name_prefix": constants.SORTED_DESC_PROFIT_SUM_BY_YEAR_MONTH__ITEM_NAME_QUEUE_PREFIX,
        "prev_controllers_amount": int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    }
    producers_config = {
        "queue_name_prefix": constants.QRS_QUEUE_PREFIX,
    }

    cleaner = Query22OutputBuilder(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        health_listen_port=int(config_params["HEALTH_LISTEN_PORT"]),
        consumers_config=consumers_config,
        producers_config=producers_config,
    )
    cleaner.run()


if __name__ == "__main__":
    main()
