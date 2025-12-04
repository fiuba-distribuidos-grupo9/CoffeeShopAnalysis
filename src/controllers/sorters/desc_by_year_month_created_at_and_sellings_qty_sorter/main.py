import logging

from controllers.sorters.desc_by_year_month_created_at_and_sellings_qty_sorter.desc_by_year_month_created_at_and_sellings_qty_sorter import (
    DescByYearMonthCreatedAtAndSellingsQtySorter,
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
        "queue_name_prefix": constants.SELLINGS_QTY_BY_YEAR_MONTH_CREATED_AT__ITEM_ID_QUEUE_PREFIX,
        "prev_controllers_amount": int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    }
    producers_config = {
        "queue_name_prefix": constants.SORTED_DESC_SELLINGS_QTY_BY_YEAR_MONTH__ITEM_ID_QUEUE_PREFIX,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = DescByYearMonthCreatedAtAndSellingsQtySorter(
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
