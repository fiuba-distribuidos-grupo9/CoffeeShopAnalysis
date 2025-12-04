import logging

from controllers.sorters.desc_by_store_id_and_purchases_qty_sorter.desc_by_store_id_and_purchases_qty_sorter import (
    DescByStoreIdAndPurchasesQtySorter,
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
            "BATCH_MAX_SIZE",
            "AMOUNT_PER_GROUP",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "queue_name_prefix": constants.PURCHASES_QTY_BY_USR_ID__STORE_ID_QUEUE_PREFIX,
        "prev_controllers_amount": int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    }
    producers_config = {
        "queue_name_prefix": constants.SORTED_DESC_BY_STORE_ID__PURCHASES_QTY_WITH_USER_ID,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = DescByStoreIdAndPurchasesQtySorter(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        health_listen_port=int(config_params["HEALTH_LISTEN_PORT"]),
        consumers_config=consumers_config,
        producers_config=producers_config,
        batch_max_size=int(config_params["BATCH_MAX_SIZE"]),
        amount_per_group=int(config_params["AMOUNT_PER_GROUP"]),
    )
    controller.run()


if __name__ == "__main__":
    main()
