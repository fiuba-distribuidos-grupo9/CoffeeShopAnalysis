from controllers.output_builders.shared.query_output_builder import QueryOutputBuilder
from shared.communication_protocol import constants


class Query21OutputBuilder(QueryOutputBuilder):

    # ============================== PRIVATE - INTERFACE ============================== #

    def _columns_to_keep(self) -> list[str]:
        return ["year_month_created_at", "item_name", "sellings_qty"]

    def _output_message_type(self) -> str:
        return constants.QUERY_RESULT_21_MSG_TYPE
