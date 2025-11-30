#!/bin/bash

# ============================== PRIVATE - UTILS ============================== #

function add-line() {
  local compose_file=$1
  local text="$2"
  echo "$text" >> "$compose_file"
}

function add-empty-line() {
  local compose_file=$1
  add-line $compose_file ''
}

function add-comment() {
  local compose_file=$1
  local comment_text=$2

  add-line $compose_file "  # ============================== $comment_text ============================== #"
  add-empty-line $compose_file
}

# ============================== PRIVATE - NAME ============================== #

function add-name() {
  local compose_file=$1
  echo 'name: tp' > "$compose_file"
}

# ============================== PRIVATE - RABBITMQ ============================== #

function add-rabbitmq-service() {
  local compose_file=$1
  add-line $compose_file '  rabbitmq-message-middleware:'
  add-line $compose_file '    container_name: rabbitmq-message-middleware'
  add-line $compose_file '    image: "rabbitmq:4-management"'
  add-line $compose_file '    ports:'
  add-line $compose_file '      - "5672:5672"'
  add-line $compose_file '      - "15672:15672"'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - RABBITMQ_DEFAULT_USER=${RABBITMQ_USER}'
  add-line $compose_file '      - RABBITMQ_DEFAULT_PASS=${RABBITMQ_PASS}'
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    healthcheck:'
  add-line $compose_file '      test: rabbitmq-diagnostics check_port_connectivity'
  add-line $compose_file '      interval: 30s'
  add-line $compose_file '      timeout: 5s'
  add-line $compose_file '      retries: 5'
  add-line $compose_file '      start_period: 30s'
}

# ============================== PRIVATE - CLIENT & SERVER ============================== #

function add-client-service() {
  local compose_file=$1
  local client=$2
  add-line $compose_file "  client_$client:"
  add-line $compose_file "    container_name: client_$client"
  add-line $compose_file '    image: client:latest'
  add-line $compose_file '    entrypoint: python3 -m client.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CLIENT_ID=$client"
  add-line $compose_file '      - SERVER_HOST=server'
  add-line $compose_file '      - SERVER_PORT=5000'
  add-line $compose_file '      - DATA_PATH=/data'
  add-line $compose_file '      - RESULTS_PATH=/results'
  add-line $compose_file '      - BATCH_MAX_SIZE=${BATCH_MAX_SIZE}'
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    volumes:'
  add-line $compose_file '      - type: bind'
  add-line $compose_file "        source: \${LOCAL_DATA_PATH_$client}"
  add-line $compose_file '        target: /data'
  add-line $compose_file '        read_only: true'
  add-line $compose_file '      - type: bind'
  add-line $compose_file '        source: ${LOCAL_RESULTS_PATH}'
  add-line $compose_file '        target: /results'
  add-line $compose_file '        read_only: false'
  add-line $compose_file '    deploy:'
  add-line $compose_file '      restart_policy:'
  add-line $compose_file '        condition: on-failure'
  add-line $compose_file '        delay: 5s'
  add-line $compose_file '        max_attempts: 1'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      server:'
  add-line $compose_file '        condition: service_started'
}

function add-client-services() {
  local compose_file=$1
  
  for ((i=0;i<$CLIENTS_AMOUNT;i++)); do
    add-client-service $compose_file $i
    add-empty-line $compose_file
  done
}

function add-server-service() {
  local compose_file=$1
  add-line $compose_file '  server:'
  add-line $compose_file '    container_name: server'
  add-line $compose_file '    image: server:latest'
  add-line $compose_file '    entrypoint: python3 -m server.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file '      - SERVER_PORT=5000'
  add-line $compose_file '      - SERVER_LISTEN_BACKLOG=${SERVER_LISTEN_BACKLOG}'
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file '      - MENU_ITEMS_CLN_AMOUNT=1'
  add-line $compose_file '      - STORES_CLN_AMOUNT=1'
  add-line $compose_file "      - TRANSACTION_ITEMS_CLN_AMOUNT=$TRANSACTION_ITEMS_CLN_AMOUNT"
  add-line $compose_file "      - TRANSACTIONS_CLN_AMOUNT=$TRANSACTIONS_CLN_AMOUNT"
  add-line $compose_file "      - USERS_CLN_AMOUNT=$USERS_CLN_AMOUNT"
  add-line $compose_file "      - Q1X_OB_AMOUNT=$Q1X_OB_AMOUNT"
  add-line $compose_file "      - Q21_OB_AMOUNT=$Q21_OB_AMOUNT"
  add-line $compose_file "      - Q22_OB_AMOUNT=$Q22_OB_AMOUNT"
  add-line $compose_file "      - Q3X_OB_AMOUNT=$Q3X_OB_AMOUNT"
  add-line $compose_file "      - Q4X_OB_AMOUNT=$Q4X_OB_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
  add-line $compose_file '      transaction_items_with_menu_items_query_21_joiner_0:'
  add-line $compose_file '        condition: service_started'
  add-line $compose_file '      transaction_items_with_menu_items_query_22_joiner_0:'
  add-line $compose_file '        condition: service_started'
  add-line $compose_file '      transactions_with_stores_query_3x_joiner_0:'
  add-line $compose_file '        condition: service_started'
  add-line $compose_file '      transactions_with_stores_query_4x_joiner_0:'
  add-line $compose_file '        condition: service_started'
}

# ============================== PRIVATE - CLEANERS ============================== #

function add-menu-cleaner() {
  local compose_file=$1
  add-line $compose_file '  menu_items_cleaner_0:'
  add-line $compose_file '    container_name: menu_items_cleaner_0'
  add-line $compose_file '    image: menu_items_cleaner:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.cleaners.menu_items_cleaner.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file '      - CONTROLLER_ID=0'
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q2_JOINERS_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-stores-cleaner() {
  local compose_file=$1
  add-line $compose_file '  stores_cleaner_0:'
  add-line $compose_file '    container_name: stores_cleaner_0'
  add-line $compose_file '    image: stores_cleaner:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.cleaners.stores_cleaner.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file '      - CONTROLLER_ID=0'
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  local greater_join_amount
  if (( $Q3_JOINERS_AMOUNT>$Q4_TRANSACTIONS_WITH_STORES_JOINERS_AMOUNT)); then
    greater_join_amount=$Q3_JOINERS_AMOUNT
  else 
    greater_join_amount=$Q4_TRANSACTIONS_WITH_STORES_JOINERS_AMOUNT
  fi
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$greater_join_amount"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-transaction-items-cleaner() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  transaction_items_cleaner_$current_id:"
  add-line $compose_file "    container_name: transaction_items_cleaner_$current_id"
  add-line $compose_file '    image: transaction_items_cleaner:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.cleaners.transaction_items_cleaner.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$FILTER_TRANSACTION_ITEMS_BY_YEAR_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-transactions-cleaner() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  transactions_cleaner_$current_id:"
  add-line $compose_file "    container_name: transactions_cleaner_$current_id"
  add-line $compose_file '    image: transactions_cleaner:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.cleaners.transactions_cleaner.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$FILTER_TRANSACTIONS_BY_YEAR_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-users-cleaner() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  users_cleaner_$current_id:"
  add-line $compose_file "    container_name: users_cleaner_$current_id"
  add-line $compose_file '    image: users_cleaner:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.cleaners.users_cleaner.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q4_TRANSACTIONS_WITH_USERS_JOINERS_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-cleaners() {
  local compose_file=$1
  
  add-menu-cleaner $compose_file
  add-empty-line $compose_file
  
  add-stores-cleaner $compose_file
  add-empty-line $compose_file
  
  for ((i=0;i<$TRANSACTION_ITEMS_CLN_AMOUNT;i++)); do
    add-transaction-items-cleaner $compose_file $i
    add-empty-line $compose_file
  done
  
  for ((i=0;i<$TRANSACTIONS_CLN_AMOUNT;i++)); do
    add-transactions-cleaner $compose_file $i
    add-empty-line $compose_file
  done
  
  for ((i=0;i<$USERS_CLN_AMOUNT;i++)); do
    add-users-cleaner $compose_file $i
    add-empty-line $compose_file
  done
}

# ============================== PRIVATE - FILTERS ============================== #

function add-transactions-filter-by-year() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  filter_transactions_by_year_$current_id:"
  add-line $compose_file "    container_name: filter_transactions_by_year_$current_id"
  add-line $compose_file '    image: filter_transactions_by_year:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.filters.filter_transactions_by_year.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$TRANSACTIONS_CLN_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$FILTER_TRANSACTIONS_BY_HOUR_AMOUNT"
  add-line $compose_file "      - YEARS_TO_KEEP=$YEARS_TO_KEEP"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-transactions-filter-by-hour() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  filter_transactions_by_hour_$current_id:"
  add-line $compose_file "    container_name: filter_transactions_by_hour_$current_id"
  add-line $compose_file '    image: filter_transactions_by_hour:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.filters.filter_transactions_by_hour.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$FILTER_TRANSACTIONS_BY_YEAR_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$FILTER_TRANSACTIONS_BY_FINAL_AMNT_AMOUNT"
  add-line $compose_file "      - MIN_HOUR=$MIN_HOUR"
  add-line $compose_file "      - MAX_HOUR=$MAX_HOUR"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-transactions-filter-by-amount() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  filter_transactions_by_final_amount_$current_id:"
  add-line $compose_file "    container_name: filter_transactions_by_final_amount_$current_id"
  add-line $compose_file '    image: filter_transactions_by_final_amount:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.filters.filter_transactions_by_final_amount.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$FILTER_TRANSACTIONS_BY_HOUR_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q1X_OB_AMOUNT"
  add-line $compose_file "      - MIN_FINAL_AMOUNT=$MIN_FINAL_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-transaction-items-filter-by-year() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  filter_transaction_items_by_year_$current_id:"
  add-line $compose_file "    container_name: filter_transaction_items_by_year_$current_id"
  add-line $compose_file '    image: filter_transaction_items_by_year:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.filters.filter_transaction_items_by_year.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$TRANSACTION_ITEMS_CLN_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$YEAR_MONTH_CREATED_AT_TRANSACTION_ITEMS_MAPPERS_AMOUNT"
  add-line $compose_file "      - YEARS_TO_KEEP=$YEARS_TO_KEEP"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-filters() {
  local compose_file=$1
  
  for((i=0;i<$FILTER_TRANSACTIONS_BY_YEAR_AMOUNT;i++)); do
    add-transactions-filter-by-year $compose_file $i
    add-empty-line $compose_file
  done
  
  for((i=0;i<$FILTER_TRANSACTIONS_BY_HOUR_AMOUNT;i++)); do
    add-transactions-filter-by-hour $compose_file $i
    add-empty-line $compose_file
  done
  
  for((i=0;i<$FILTER_TRANSACTIONS_BY_FINAL_AMNT_AMOUNT;i++)); do
    add-transactions-filter-by-amount $compose_file $i
    add-empty-line $compose_file
  done
  
  for((i=0;i<$FILTER_TRANSACTION_ITEMS_BY_YEAR_AMOUNT;i++)); do
    add-transaction-items-filter-by-year $compose_file $i
    add-empty-line $compose_file
  done
}

# ============================== PRIVATE - MAPPERS ============================== #

function add-year-month-created-at-mapper() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  year_month_created_at_transaction_items_mapper_$current_id:"
  add-line $compose_file "    container_name: year_month_created_at_transaction_items_mapper_$current_id"
  add-line $compose_file '    image: year_month_created_at_transaction_items_mapper:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.mappers.year_month_created_at_transaction_items_mapper.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$FILTER_TRANSACTION_ITEMS_BY_YEAR_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q2_REDUCERS_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-year-half-created-at-mapper() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  year_half_created_at_transactions_mapper_$current_id:"
  add-line $compose_file "    container_name: year_half_created_at_transactions_mapper_$current_id"
  add-line $compose_file '    image: year_half_created_at_transactions_mapper:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.mappers.year_half_created_at_transactions_mapper.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$FILTER_TRANSACTIONS_BY_HOUR_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q3_REDUCERS_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-mappers() {
  local compose_file=$1
  
  for ((i=0;i<$YEAR_MONTH_CREATED_AT_TRANSACTION_ITEMS_MAPPERS_AMOUNT;i++)); do
    add-year-month-created-at-mapper $compose_file $i
    add-empty-line $compose_file
  done
  
  for ((i=0;i<$YEAR_HALF_CREATED_AT_TRANSACTIONS_MAPPERS_AMOUNT;i++)); do
    add-year-half-created-at-mapper $compose_file $i
    add-empty-line $compose_file
  done
}

# ============================== PRIVATE - REDUCERS ============================== #

function add-selling-qty-reducer() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  sellings_qty_by_item_id_and_year_month_created_at_reducer_$current_id:"
  add-line $compose_file "    container_name: sellings_qty_by_item_id_and_year_month_created_at_reducer_$current_id"
  add-line $compose_file '    image: sellings_qty_by_item_id_and_year_month_created_at_reducer:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.reducers.sellings_qty_by_item_id_and_year_month_created_at_reducer.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$YEAR_MONTH_CREATED_AT_TRANSACTION_ITEMS_MAPPERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q2_SORTERS_AMOUNT"
  add-line $compose_file '      - BATCH_MAX_SIZE=${BATCH_MAX_SIZE}'
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-profit-sum-reducer() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  profit_sum_by_item_id_and_year_month_created_at_reducer_$current_id:"
  add-line $compose_file "    container_name: profit_sum_by_item_id_and_year_month_created_at_reducer_$current_id"
  add-line $compose_file '    image: profit_sum_by_item_id_and_year_month_created_at_reducer:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.reducers.profit_sum_by_item_id_and_year_month_created_at_reducer.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$YEAR_MONTH_CREATED_AT_TRANSACTION_ITEMS_MAPPERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q2_SORTERS_AMOUNT"
  add-line $compose_file '      - BATCH_MAX_SIZE=${BATCH_MAX_SIZE}'
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-tpv-by-store-reducer() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  tpv_by_store_id_and_year_half_created_at_reducer_$current_id:"
  add-line $compose_file "    container_name: tpv_by_store_id_and_year_half_created_at_reducer_$current_id"
  add-line $compose_file '    image: tpv_by_store_id_and_year_half_created_at_reducer:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.reducers.tpv_by_store_id_and_year_half_created_at_reducer.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$YEAR_HALF_CREATED_AT_TRANSACTIONS_MAPPERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q3_JOINERS_AMOUNT"
  add-line $compose_file '      - BATCH_MAX_SIZE=${BATCH_MAX_SIZE}'
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-user-purchase-by-store-reducer() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  purchases_qty_by_store_id_and_user_id_reducer_$current_id:"
  add-line $compose_file "    container_name: purchases_qty_by_store_id_and_user_id_reducer_$current_id"
  add-line $compose_file '    image: purchases_qty_by_store_id_and_user_id_reducer:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.reducers.purchases_qty_by_store_id_and_user_id_reducer.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$FILTER_TRANSACTIONS_BY_YEAR_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q4_SORTERS_AMOUNT"
  add-line $compose_file '      - BATCH_MAX_SIZE=${BATCH_MAX_SIZE}'
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-reducers() {
  local compose_file=$1

  for ((i=0;i<$Q2_REDUCERS_AMOUNT;i++)); do
    add-selling-qty-reducer $compose_file $i
    add-empty-line $compose_file
  done

  for ((i=0;i<$Q2_REDUCERS_AMOUNT;i++)); do
    add-profit-sum-reducer $compose_file $i
    add-empty-line $compose_file
  done

  for ((i=0;i<$Q3_REDUCERS_AMOUNT;i++)); do
    add-tpv-by-store-reducer $compose_file $i
    add-empty-line $compose_file
  done

  for ((i=0;i<$Q4_REDUCERS_AMOUNT;i++)); do
    add-user-purchase-by-store-reducer $compose_file $i
    add-empty-line $compose_file
  done
}

# ============================== PRIVATE - SORTERS ============================== #

function add-selling-qty-sorter(){
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  desc_by_year_month_created_at_and_sellings_qty_sorter_$current_id:"
  add-line $compose_file "    container_name: desc_by_year_month_created_at_and_sellings_qty_sorter_$current_id"
  add-line $compose_file '    image: desc_by_year_month_created_at_and_sellings_qty_sorter:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.sorters.desc_by_year_month_created_at_and_sellings_qty_sorter.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$Q2_REDUCERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q2_JOINERS_AMOUNT"
  add-line $compose_file '      - BATCH_MAX_SIZE=${BATCH_MAX_SIZE}'
  add-line $compose_file '      - AMOUNT_PER_GROUP=1'
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-profit-sum-sorter(){
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  desc_by_year_month_created_at_and_profit_sum_sorter_$current_id:"
  add-line $compose_file "    container_name: desc_by_year_month_created_at_and_profit_sum_sorter_$current_id"
  add-line $compose_file '    image: desc_by_year_month_created_at_and_profit_sum_sorter:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.sorters.desc_by_year_month_created_at_and_profit_sum_sorter.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$Q2_REDUCERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q2_JOINERS_AMOUNT"
  add-line $compose_file '      - BATCH_MAX_SIZE=${BATCH_MAX_SIZE}'
  add-line $compose_file '      - AMOUNT_PER_GROUP=1'
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-user-purchase-by-store-sorter(){
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  desc_by_store_id_and_purchases_qty_sorter_$current_id:"
  add-line $compose_file "    container_name: desc_by_store_id_and_purchases_qty_sorter_$current_id"
  add-line $compose_file '    image: desc_by_store_id_and_purchases_qty_sorter:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.sorters.desc_by_store_id_and_purchases_qty_sorter.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$Q4_REDUCERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q4_TRANSACTIONS_WITH_USERS_JOINERS_AMOUNT"
  add-line $compose_file '      - BATCH_MAX_SIZE=${BATCH_MAX_SIZE}'
  add-line $compose_file '      - AMOUNT_PER_GROUP=3'
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-sorters() {
  local compose_file=$1

  for ((i=0;i<$Q2_SORTERS_AMOUNT;i++)); do
    add-selling-qty-sorter $compose_file $i
    add-empty-line $compose_file
  done

  for ((i=0;i<$Q2_SORTERS_AMOUNT;i++)); do
    add-profit-sum-sorter $compose_file $i
    add-empty-line $compose_file
  done

  for ((i=0;i<$Q4_SORTERS_AMOUNT;i++)); do
    add-user-purchase-by-store-sorter $compose_file $i
    add-empty-line $compose_file
  done
}

# ============================== PRIVATE - JOINERS ============================== #

function add-menu-with-items-q21-joiner(){
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  transaction_items_with_menu_items_query_21_joiner_$current_id:"
  add-line $compose_file "    container_name: transaction_items_with_menu_items_query_21_joiner_$current_id"
  add-line $compose_file '    image: transaction_items_with_menu_items_query_21_joiner:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.joiners.transaction_items_with_menu_items_joiner.transaction_items_with_menu_items_query_21_joiner.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file '      - BASE_DATA_PREV_CONTROLLERS_AMOUNT=1'
  add-line $compose_file "      - STREAM_DATA_PREV_CONTROLLERS_AMOUNT=$Q2_SORTERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q21_OB_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-menu-with-items-q22-joiner(){
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  transaction_items_with_menu_items_query_22_joiner_$current_id:"
  add-line $compose_file "    container_name: transaction_items_with_menu_items_query_22_joiner_$current_id"
  add-line $compose_file '    image: transaction_items_with_menu_items_query_22_joiner:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.joiners.transaction_items_with_menu_items_joiner.transaction_items_with_menu_items_query_22_joiner.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file '      - BASE_DATA_PREV_CONTROLLERS_AMOUNT=1'
  add-line $compose_file "      - STREAM_DATA_PREV_CONTROLLERS_AMOUNT=$Q2_SORTERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q22_OB_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-transactions-with-stores-q3x-joiner(){
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  transactions_with_stores_query_3x_joiner_$current_id:"
  add-line $compose_file "    container_name: transactions_with_stores_query_3x_joiner_$current_id"
  add-line $compose_file '    image: transactions_with_stores_query_3x_joiner:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.joiners.transactions_with_stores_joiner.transactions_with_stores_query_3x_joiner.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - OUTPUT_BUILDERS_AMOUNT=$Q3X_OB_AMOUNT"
  add-line $compose_file '      - BASE_DATA_PREV_CONTROLLERS_AMOUNT=1' 
  add-line $compose_file "      - STREAM_DATA_PREV_CONTROLLERS_AMOUNT=$Q3_REDUCERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q3X_OB_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-transactions-with-stores-q4x-joiner(){
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  transactions_with_stores_query_4x_joiner_$current_id:"
  add-line $compose_file "    container_name: transactions_with_stores_query_4x_joiner_$current_id"
  add-line $compose_file '    image: transactions_with_stores_query_4x_joiner:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.joiners.transactions_with_stores_joiner.transactions_with_stores_query_4x_joiner.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file '      - BASE_DATA_PREV_CONTROLLERS_AMOUNT=1' 
  add-line $compose_file "      - STREAM_DATA_PREV_CONTROLLERS_AMOUNT=$Q4_TRANSACTIONS_WITH_USERS_JOINERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q4X_OB_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-transactions-with-users-q4x-joiner(){
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  transactions_with_users_joiner_$current_id:"
  add-line $compose_file "    container_name: transactions_with_users_joiner_$current_id"
  add-line $compose_file '    image: transactions_with_users_joiner:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.joiners.transactions_with_users_joiner.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - BASE_DATA_PREV_CONTROLLERS_AMOUNT=$USERS_CLN_AMOUNT"
  add-line $compose_file "      - STREAM_DATA_PREV_CONTROLLERS_AMOUNT=$Q4_REDUCERS_AMOUNT"
  add-line $compose_file "      - NEXT_CONTROLLERS_AMOUNT=$Q4_TRANSACTIONS_WITH_STORES_JOINERS_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-joiners() {
  local compose_file=$1
  
  for ((i=0;i<$Q2_JOINERS_AMOUNT;i++)); do
    add-menu-with-items-q21-joiner $compose_file $i
    add-empty-line $compose_file
  done
  
  for ((i=0;i<$Q2_JOINERS_AMOUNT;i++)); do
    add-menu-with-items-q22-joiner $compose_file $i
    add-empty-line $compose_file
  done
  
  for ((i=0;i<$Q3_JOINERS_AMOUNT;i++)); do
    add-transactions-with-stores-q3x-joiner $compose_file $i
    add-empty-line $compose_file
  done
  
  for ((i=0;i<$Q4_TRANSACTIONS_WITH_STORES_JOINERS_AMOUNT;i++)); do
    add-transactions-with-stores-q4x-joiner $compose_file $i
    add-empty-line $compose_file
  done
  
  for ((i=0;i<$Q4_TRANSACTIONS_WITH_USERS_JOINERS_AMOUNT;i++)); do
    add-transactions-with-users-q4x-joiner $compose_file $i
    add-empty-line $compose_file
  done  
}

# ============================== PRIVATE - OUTPUT BUILDER ============================== #

function add-query-1x-output-builder() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  query_1x_output_builder_$current_id:"
  add-line $compose_file "    container_name: query_1x_output_builder_$current_id"
  add-line $compose_file '    image: query_1x_output_builder:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.output_builders.query_1x_output_builder.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$FILTER_TRANSACTIONS_BY_FINAL_AMNT_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-query-21-output-builder() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  query_21_output_builder_$current_id:"
  add-line $compose_file "    container_name: query_21_output_builder_$current_id"
  add-line $compose_file '    image: query_21_output_builder:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.output_builders.query_21_output_builder.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$Q2_JOINERS_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-query-22-output-builder() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  query_22_output_builder_$current_id:"
  add-line $compose_file "    container_name: query_22_output_builder_$current_id"
  add-line $compose_file '    image: query_22_output_builder:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.output_builders.query_22_output_builder.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$Q2_JOINERS_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-query-3x-output-builder() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  query_3x_output_builder_$current_id:"
  add-line $compose_file "    container_name: query_3x_output_builder_$current_id"
  add-line $compose_file '    image: query_3x_output_builder:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.output_builders.query_3x_output_builder.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$Q3_JOINERS_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-query-4x-output-builder() {
  local compose_file=$1
  local current_id=$2
  add-line $compose_file "  query_4x_output_builder_$current_id:"
  add-line $compose_file "    container_name: query_4x_output_builder_$current_id"
  add-line $compose_file '    image: query_4x_output_builder:latest'
  add-line $compose_file '    entrypoint: python3 -m controllers.output_builders.query_4x_output_builder.main'
  add-line $compose_file '    environment:'
  add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
  add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
  add-line $compose_file "      - CONTROLLER_ID=$current_id"
  add-line $compose_file '      - RABBITMQ_HOST=rabbitmq-message-middleware'
  add-line $compose_file "      - PREV_CONTROLLERS_AMOUNT=$Q4_TRANSACTIONS_WITH_STORES_JOINERS_AMOUNT"
  add-line $compose_file '    networks:'
  add-line $compose_file '      - custom_net'
  add-line $compose_file '    depends_on:'
  add-line $compose_file '      rabbitmq-message-middleware:'
  add-line $compose_file '        condition: service_healthy'
}

function add-output-builders() {
  local compose_file=$1
  
  for ((i=0;i<$Q1X_OB_AMOUNT;i++)); do
    add-query-1x-output-builder $compose_file $i
    add-empty-line $compose_file 
  done
  
  for ((i=0;i<$Q21_OB_AMOUNT;i++)); do
    add-query-21-output-builder $compose_file $i
    add-empty-line $compose_file
  done
  
  for ((i=0;i<$Q22_OB_AMOUNT;i++)); do
    add-query-22-output-builder $compose_file $i
    add-empty-line $compose_file
  done
  
  for ((i=0;i<$Q3X_OB_AMOUNT;i++)); do
    add-query-3x-output-builder $compose_file $i
    add-empty-line $compose_file
  done
  
  for ((i=0;i<$Q4X_OB_AMOUNT;i++)); do
    add-query-4x-output-builder $compose_file $i
    add-empty-line $compose_file
  done
}

# ============================== PRIVATE - HEALTH CHECKERS ============================== #

function get-targets(){
  local targets="server,menu_items_cleaner_0,stores_cleaner_0,"
    for ((j = 0; j < $TRANSACTION_ITEMS_CLN_AMOUNT; j++)) do
      targets="${targets}transaction_items_cleaner_$j,"
    done
    for ((j = 0; j < $TRANSACTIONS_CLN_AMOUNT; j++)) do
      targets="${targets}transactions_cleaner_$j,"
    done
    for ((j = 0; j < $USERS_CLN_AMOUNT; j++)) do 
      targets="${targets}users_cleaners_$j,"
    done
    for ((j = 0; j < $FILTER_TRANSACTIONS_BY_YEAR_AMOUNT; j++)) do 
      targets="${targets}filter_transactions_by_year_$j,"
    done
    for ((j = 0; j < $FILTER_TRANSACTIONS_BY_HOUR_AMOUNT; j++)) do 
      targets="${targets}filter_transactions_by_hour_$j,"
    done
    for ((j = 0; j < $FILTER_TRANSACTIONS_BY_FINAL_AMNT_AMOUNT; j++)) do 
      targets="${targets}filter_transactions_by_final_amount_$j,"
    done
    for ((j = 0; j < $FILTER_TRANSACTION_ITEMS_BY_YEAR_AMOUNT; j++)) do 
      targets="${targets}filter_transaction_items_by_year_$j,"
    done
    for ((j = 0; j < $YEAR_MONTH_CREATED_AT_TRANSACTION_ITEMS_MAPPERS_AMOUNT; j++)) do 
      targets="${targets}year_month_created_at_transaction_items_mapper_$j,"
    done
    for ((j = 0; j < $YEAR_HALF_CREATED_AT_TRANSACTIONS_MAPPERS_AMOUNT; j++)) do 
      targets="${targets}year_half_created_at_transactions_mapper_$j,"
    done
    for ((j = 0; j < $YEAR_HALF_CREATED_AT_TRANSACTIONS_MAPPERS_AMOUNT; j++)) do 
      targets="${targets}year_half_created_at_transactions_mapper_$j,"
    done
    for ((j = 0; j < $Q2_REDUCERS_AMOUNT; j++)) do 
      targets="${targets}sellings_qty_by_item_id_and_year_month_created_at_reducer_$j,"
      targets="${targets}profit_sum_by_item_id_and_year_month_created_at_reducer_$j,"
    done
    for ((j = 0; j < $Q3_REDUCERS_AMOUNT; j++)) do 
      targets="${targets}tpv_by_store_id_and_year_half_created_at_reducer_$j,"
    done
    for ((j = 0; j < $Q4_REDUCERS_AMOUNT; j++)) do 
      targets="${targets}purchases_qty_by_store_id_and_user_id_reducer_$j,"
    done
    for ((j = 0; j < $Q2_SORTERS_AMOUNT; j++)) do 
      targets="${targets}desc_by_year_month_created_at_and_sellings_qty_sorter_$j,"
      targets="${targets}desc_by_year_month_created_at_and_profit_sum_sorter_$j,"
    done
    for ((j = 0; j < $Q4_SORTERS_AMOUNT; j++)) do 
      targets="${targets}desc_by_store_id_and_purchases_qty_sorter_$j,"
    done
    for ((j = 0; j < $Q2_JOINERS_AMOUNT; j++)) do 
      targets="${targets}transaction_items_with_menu_items_query_21_joiner_$j,"
      targets="${targets}transaction_items_with_menu_items_query_22_joiner_$j,"
    done
    for ((j = 0; j < $Q3_JOINERS_AMOUNT; j++)) do 
      targets="${targets}transactions_with_stores_query_3x_joiner_$j,"
    done
    for ((j = 0; j < $Q4_TRANSACTIONS_WITH_STORES_JOINERS_AMOUNT; j++)) do 
      targets="${targets}transactions_with_stores_query_4x_joiner_$j,"
    done
    for ((j = 0; j < $Q4_TRANSACTIONS_WITH_USERS_JOINERS_AMOUNT; j++)) do 
      targets="${targets}transactions_with_users_joiner_$j,"
    done
    for ((j = 0; j < $Q1X_OB_AMOUNT; j++)) do 
      targets="${targets}query_1x_output_builder_$j,"
    done
    for ((j = 0; j < $Q21_OB_AMOUNT; j++)) do 
      targets="${targets}query_21_output_builder_$j,"
    done
    for ((j = 0; j < $Q22_OB_AMOUNT; j++)) do 
      targets="${targets}query_22_output_builder_$j,"
    done
    for ((j = 0; j < $Q3X_OB_AMOUNT; j++)) do 
      targets="${targets}query_3x_output_builder_$j,"
    done
    for ((j = 0; j < $Q4X_OB_AMOUNT; j++)) do 
      targets="${targets}query_4x_output_builder_$j,"
    done
    targets="${targets%,}"
    echo $targets
}



function add-health-checkers() {
  for ((i = 0; i < $HC_AMOUNT; i++)); do
    add-line $compose_file "  hc_$i:"
    add-line $compose_file "    container_name: hc_$i"
    add-line $compose_file "    image: ${HC_IMAGE_NAME}"
    add-line $compose_file '    volumes: '
    add-line $compose_file '      - /var/run/docker.sock:/var/run/docker.sock'
    add-line $compose_file '    environment:'
    add-line $compose_file "      - NODE_ID=$i"
    add-line $compose_file "      - NODE_NAME=hc_$i"
    add-line $compose_file "      - LISTEN_PORT=${ELECTION_PORT}"
    add-line $compose_file "      - HEALTH_LISTEN_PORT=${HEALTH_LISTEN_PORT}"
    local peers=""
    for ((j = 0; j < HC_AMOUNT; j++)); do
      if [[ $j -eq $i ]]; then
        continue
      fi
      peers="${peers}hc_$j,"
    done
    peers="${peers%,}"
    targets=$(get-targets)
    add-line $compose_file "      - RING_PEERS=$peers"
    add-line $compose_file "      - CONTROLLER_TARGETS=$targets"
    add-line $compose_file '      - LOGGING_LEVEL=${LOGGING_LEVEL}'
    add-line $compose_file '      - PYTHONUNBUFFERED=${PYTHONUNBUFFERED}'
    add-line $compose_file "      - HEARTBEAT_INTERVAL_MS=${HEARTBEAT_INTERVAL_MS}"
    add-line $compose_file "      - HEARTBEAT_TIMEOUT_MS=${HEARTBEAT_TIMEOUT_MS}"
    add-line $compose_file "      - NODHEARTBEAT_MAX_RETRIES=${HEARTBEAT_MAX_RETRIES}"
    add-line $compose_file "      - SUSPECT_GRACE_MS=${SUSPECT_GRACE_MS}"
    add-line $compose_file "      - LEADER_CHECK_INTERVAL_MS=${LEADER_CHECK_INTERVAL_MS}"
    add-line $compose_file "      - LEADER_CHECK_TIMEOUT_MS=${LEADER_CHECK_TIMEOUT_MS}"
    add-line $compose_file "      - ELECTION_BACKOFF_MS_MIN=${ELECTION_BACKOFF_MS_MIN}"
    add-line $compose_file "      - ELECTION_BACKOFF_MS_MAX=${ELECTION_BACKOFF_MS_MAX}"
    add-line $compose_file "      - LEADER_RANDOM_SLEEP_MIN_MS=${LEADER_RANDOM_SLEEP_MIN_MS}"
    add-line $compose_file "      - LEADER_RANDOM_SLEEP_MAX_MS=${LEADER_RANDOM_SLEEP_MAX_MS}"
    add-line $compose_file "      - DOCKER_HOST=${DOCKER_HOST}"                               
    add-line $compose_file '    networks:'
    add-line $compose_file '      - custom_net'
    add-line $compose_file '    depends_on:'
    add-line $compose_file '      rabbitmq-message-middleware:'
    add-line $compose_file '        condition: service_healthy'
  done

}


# ============================== PRIVATE - SERVICES ============================== #

function add-services() {
  local compose_file=$1
  
  add-line $compose_file 'services:'
  add-empty-line $compose_file

  add-comment $compose_file 'RABBITMQ SERVICE'
  add-rabbitmq-service $compose_file
  add-empty-line $compose_file
  
  add-comment $compose_file 'CLIENT & SERVER SERVICES'
  add-client-services $compose_file
  add-server-service $compose_file
  add-empty-line $compose_file

  add-comment $compose_file 'CLEANERS SERVICES'
  add-cleaners $compose_file
  add-empty-line $compose_file

  add-comment $compose_file 'FILTERS SERVICES'
  add-filters $compose_file

  add-comment $compose_file 'MAPPERS SERVICES'
  add-mappers $compose_file

  add-comment $compose_file 'REDUCERS SERVICES'
  add-reducers $compose_file

  add-comment $compose_file 'SORTERS SERVICES'
  add-sorters $compose_file

  add-comment $compose_file 'JOINERS SERVICES'
  add-joiners $compose_file

  add-comment $compose_file 'OUTPUT BUILDERS SERVICES'
  add-output-builders $compose_file

  # add-comment $compose_file 'HEALTH CHECKERS SERVICES'
  # add-health-checkers
}

# ============================== PRIVATE - NETWORKS ============================== #

function add-networks() {
  local compose_file=$1
  
  add-line $compose_file 'networks:'
  add-line $compose_file '  custom_net:'
  add-line $compose_file '    ipam:'
  add-line $compose_file '      driver: default'
  add-line $compose_file '      config:'
  add-line $compose_file '        - subnet: 172.25.125.0/24'
}

# ============================== PRIVATE - DOCKER COMPOSE FILE BUILD ============================== #

function build-docker-compose-file() {
  local compose_file=$1
  
  echo "Generando archivo $compose_file ..."
  
  add-name $compose_file
  add-services $compose_file
  add-networks $compose_file

  echo "Generando archivo $compose_file ... [DONE]"
}

# ============================== MAIN ============================== #

# take .env variables
if [ -f .env ]; then
  export $(cat .env | grep -v '#' | awk '/=/ {print $1}')
fi

if [ $# -ne 1 ]; then
  echo "Uso: $0 <compose_filename.yaml>"
  exit 1
fi

compose_filename_param=$1

echo "Nombre del archivo de salida: $compose_filename_param"

build-docker-compose-file $compose_filename_param