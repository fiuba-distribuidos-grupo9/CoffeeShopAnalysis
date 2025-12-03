from controllers.output_builders.shared.query_output_builder import QueryOutputBuilder
from shared.communication_protocol import constants


class Query4XOutputBuilder(QueryOutputBuilder):

    # ============================== PRIVATE - INTERFACE ============================== #

    def _columns_to_keep(self) -> list[str]:
        return ["store_name", "birthdate", "purchases_qty"]

    def _output_message_type(self) -> str:
        return constants.QUERY_RESULT_4X_MSG_TYPE
