import logging

from controllers.output_builders.query_1x_output_builder.query_1x_output_builder import (
    Query1XOutputBuilder,
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
        "queue_name_prefix": constants.FILTERED_TRN_BY_YEAR__HOUR__FINAL_AMOUNT_QUEUE_PREFIX,
        "prev_controllers_amount": int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    }
    producers_config = {
        "queue_name_prefix": constants.QRS_QUEUE_PREFIX,
    }

    cleaner = Query1XOutputBuilder(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        health_listen_port=int(config_params["HEALTH_LISTEN_PORT"]),
        consumers_config=consumers_config,
        producers_config=producers_config,
    )
    cleaner.run()


if __name__ == "__main__":
    main()
