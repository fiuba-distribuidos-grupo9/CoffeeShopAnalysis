import logging

from controllers.reducers.sellings_qty_by_item_id_and_year_month_created_at_reducer.sellings_qty_by_item_id_and_year_month_created_at_reducer import (
    SellingsQtyByItemIdAndYearMonthCreatedAtReducer,
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
            "BATCH_MAX_SIZE",
        ]
    )
    initializer.init_log(config_params["LOGGING_LEVEL"])
    logging.info(f"action: init_config | result: success | params: {config_params}")

    consumers_config = {
        "queue_name_prefix": constants.MAPPED_YEAR_MONTH_TIT_QUEUE_PREFIX,
        "queue_name_type": "count_items",
        "prev_controllers_amount": int(config_params["PREV_CONTROLLERS_AMOUNT"]),
    }
    producers_config = {
        "queue_name_prefix": constants.SELLINGS_QTY_BY_YEAR_MONTH_CREATED_AT__ITEM_ID_QUEUE_PREFIX,
        "next_controllers_amount": int(config_params["NEXT_CONTROLLERS_AMOUNT"]),
    }

    controller = SellingsQtyByItemIdAndYearMonthCreatedAtReducer(
        controller_id=int(config_params["CONTROLLER_ID"]),
        rabbitmq_host=config_params["RABBITMQ_HOST"],
        consumers_config=consumers_config,
        producers_config=producers_config,
        batch_max_size=int(config_params["BATCH_MAX_SIZE"]),
    )
    controller.run()


if __name__ == "__main__":
    main()
