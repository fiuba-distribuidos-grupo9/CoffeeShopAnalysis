from shared.communication_protocol import constants

# ============================== COMMON CONSTANTS ============================== #

KiB = 1024

# ============================== COMMON TAGS ============================== #

MENU_ITEMS = constants.MENU_ITEMS_BATCH_MSG_TYPE
STORES = constants.STORES_BATCH_MSG_TYPE
TRANSACTION_ITEMS = constants.TRANSACTION_ITEMS_BATCH_MSG_TYPE
TRANSACTIONS = constants.TRANSACTIONS_BATCH_MSG_TYPE
USERS = constants.USERS_BATCH_MSG_TYPE

QUERY_RESULT_1X = constants.QUERY_RESULT_1X_MSG_TYPE
QUERY_RESULT_21 = constants.QUERY_RESULT_21_MSG_TYPE
QUERY_RESULT_22 = constants.QUERY_RESULT_22_MSG_TYPE
QUERY_RESULT_3X = constants.QUERY_RESULT_3X_MSG_TYPE
QUERY_RESULT_4X = constants.QUERY_RESULT_4X_MSG_TYPE

QUEUE_PREFIX = "queue_prefix_name"
WORKERS_AMOUNT = "workers_amount"

# ============================== FOLDER NAMES ============================== #

MIT_FOLDER_NAME = "menu_items"
STR_FOLDER_NAME = "stores"
TIT_FOLDER_NAME = "transaction_items"
TRN_FOLDER_NAME = "transactions"
USR_FOLDER_NAME = "users"
QRS_FOLDER_NAME = "query_results"

# ============================== MOM ============================== #

# dirty data

DIRTY_MIT_QUEUE_PREFIX = "dirty-menu-items-queue"
DIRTY_STR_QUEUE_PREFIX = "dirty-stores-queue"
DIRTY_TIT_QUEUE_PREFIX = "dirty-transaction-items-queue"
DIRTY_TRN_QUEUE_PREFIX = "dirty-transactions-queue"
DIRTY_USR_QUEUE_PREFIX = "dirty-users-queue"

# cleaners

CLEANED_TIT_QUEUE_PREFIX = "cleaned-transaction-items"
CLEANED_TRN_QUEUE_PREFIX = "cleaned-transactions"
CLEANED_USR_QUEUE_PREFIX = "cleaned-users"
CLEANED_MIT_EXCHANGE_PREFIX = "cleaned-menu-items-exchange"
CLEANED_MIT_QUEUE_PREFIX = "cleaned-menu-items-queue"
CLEANED_MIT_ROUTING_KEY_PREFIX = "cleaned-menu-items-routing-key"
CLEANED_STR_EXCHANGE_PREFIX = "cleaned_stores-exchange"
CLEANED_STR_ROUTING_KEY_PREFIX = "cleaned-stores-routing-key"

# query 1

FILTERED_TRN_BY_YEAR_EXCHANGE_PREFIX = "Q1X__trn-filtered-transactions-by-year-exchange"
FILTERED_TRN_BY_YEAR_ROUTING_KEY_PREFIX = (
    "Q1X__trn-filtered-transactions-by-year-routing-key"
)
FILTERED_TRN_BY_YEAR__HOUR_EXCHANGE_PREFIX = (
    "Q1X__trn-filtered-transactions-by-year-&-hour-exchange"
)
FILTERED_TRN_BY_YEAR__HOUR_ROUTING_KEY_PREFIX = (
    "Q1X__trn-filtered-transactions-by-year-&-hour-routing-key"
)
FILTERED_TRN_BY_YEAR__HOUR__FINAL_AMOUNT_QUEUE_PREFIX = (
    "Q1X__trn-filtered-transactions-by-year-&-time-&-final-amount"
)

# query 2

FILTERED_TIT_BY_YEAR_QUEUE_PREFIX = "Q2X__tit-filtered-transaction-items-by-year-queue"

MAPPED_YEAR_MONTH_TIT_EXHCHANGE_PREFIX = (
    "Q2X__tit-mapped-year-month-transaction-items-exchange"
)

MAPPED_YEAR_MONTH_TIT_QUEUE_PREFIX = "Q2X_tit-mapped-year-month-transaction-items-queue"

MAPPED_YEAR_MONTH_TIT_ROUTING_KEY_PREFIX = (
    "Q2X__tit-mapped-year-month-transaction-items-routing-key"
)

# query 2.1

SELLINGS_QTY_BY_YEAR_MONTH_CREATED_AT__ITEM_ID_QUEUE_PREFIX = (
    "Q21__tit-sellings-qty-by-year-month-created-at-&-item-id-queue"
)
SORTED_DESC_SELLINGS_QTY_BY_YEAR_MONTH__ITEM_ID_QUEUE_PREFIX = (
    "Q21__tit-sorted-desc-sellings-qty-by-year-month-&-item-id"
)
SORTED_DESC_SELLINGS_QTY_BY_YEAR_MONTH__ITEM_NAME_QUEUE_PREFIX = (
    "Q21__tit-sorted-desc-sellings-qty-by-year-month-&-item-name"
)

# query 2.2

PROFIT_SUM_BY_YEAR_MONTH__ITEM_ID_CREATED_AT_QUEUE_PREFIX = (
    "Q22__tit-profit-sum-by-year-month-created-at-&-item-id-queue"
)
SORTED_DESC_PROFIT_SUM_BY_YEAR_MONTH__ITEM_ID_QUEUE_PREFIX = (
    "Q22__tit-sorted-desc-profit-sum-by-year-month-&-item-id"
)
SORTED_DESC_PROFIT_SUM_BY_YEAR_MONTH__ITEM_NAME_QUEUE_PREFIX = (
    "Q22__tit-sorted-desc-profit-sum-by-year-month-&-item-name"
)

# query 3

MAPPED_TRN_SEMESTER_QUEUE_PREFIX = "Q3X__mapped-year-semester-transaction-queue"

SUM_TRN_TPV_BY_STORE_QUEUE_PREFIX = "Q3X__sum-trn-tpv-by-store-queue"

TPV_BY_HALF_YEAR_CREATED_AT__STORE_NAME_QUEUE_PREFIX = (
    "Q3X__trn-tpv-by-half-year-created-at-&-store-name"
)

# query 4

PURCHASES_QTY_BY_USR_ID__STORE_ID_QUEUE_PREFIX = (
    "Q4X__trn-purchases-qty-by-user-id-&-store-id"
)

SORTED_DESC_BY_STORE_ID__PURCHASES_QTY_WITH_USER_ID = (
    "Q4X__trn-sorted-desc-by-store-id-&-purchases-qty-with-user-id"
)
SORTED_DESC_BY_STORE_ID__PURCHASES_QTY_WITH_USER_BITHDATE = (
    "Q4X__trn-sorted-desc-by-store-id-&-purchases-qty-with-user-birthdate"
)
SORTED_DESC_BY_STORE_NAME__PURCHASES_QTY_WITH_USER_BITHDATE = (
    "Q4X__trn-sorted-desc-by-store-name-&-purchases-qty-with-user-birthdate"
)

# query results

QRS_QUEUE_PREFIX = "QXX__query-results-queue"
