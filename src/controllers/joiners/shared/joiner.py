import threading
from abc import abstractmethod
from typing import Any, Optional

from controllers.joiners.shared.base_data_handler import BaseDataHandler
from controllers.joiners.shared.stream_data_handler import StreamDataHandler
from controllers.shared.controller import Controller
from middleware.middleware import MessageMiddleware
from shared.communication_protocol.batch_message import BatchMessage


class Joiner(Controller):

    @abstractmethod
    def _build_mom_base_data_consumer(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _build_mom_stream_data_consumer(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> MessageMiddleware:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _build_mom_producer_using(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
        producer_id: int,
    ) -> MessageMiddleware:
        raise NotImplementedError("subclass responsibility")

    def _init_mom_consumers(
        self,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
    ) -> None:
        # do nothing
        pass

    def _init_mom_producers(
        self,
        rabbitmq_host: str,
        producers_config: dict[str, Any],
    ) -> None:
        # do nothing
        pass

    def __init__(
        self,
        controller_id: int,
        rabbitmq_host: str,
        consumers_config: dict[str, Any],
        producers_config: dict[str, Any],
    ) -> None:
        super().__init__(
            controller_id,
            rabbitmq_host,
            consumers_config,
            producers_config,
        )

        self._controller_id = controller_id
        self._rabbitmq_host = rabbitmq_host
        self._consumers_config = consumers_config
        self._producers_config = producers_config

        self._base_data_by_session_id: dict[str, list[BatchMessage]] = {}
        self._base_data_by_session_id_lock = threading.Lock()

        self._all_base_data_received: dict[str, bool] = {}
        self._all_base_data_received_lock = threading.Lock()

        self._base_data_handler: Optional[BaseDataHandler] = None
        self._stream_data_handler: Optional[StreamDataHandler] = None
        self._base_data_thread: threading.Thread = threading.Thread(
            target=self._handle_base_data,
            name="base_data_handler",
        )
        self._stream_data_thread: threading.Thread = threading.Thread(
            target=self._handle_stream_data,
            name="stream_data_handler",
        )

        self._uncaught_exception: Optional[Exception] = None
        self._uncaught_exception_lock = threading.Lock()

    # ============================== PRIVATE - SIGNAL HANDLER ============================== #

    def _stop(self) -> None:
        if self._base_data_handler:
            self._base_data_handler.mom_consumer().schedule_stop_sonsuming()
            self._log_info("action: base_data_consumer_stop | result: success")

        if self._stream_data_handler:
            self._stream_data_handler.mom_consumer().schedule_stop_sonsuming()
            self._log_info("action: stream_data_consumer_stop | result: success")

    # ============================== PRIVATE - ACCESSING ============================== #

    @abstractmethod
    def _join_key(self) -> str:
        raise NotImplementedError("subclass responsibility")

    @abstractmethod
    def _transform_function(self, value: str) -> Any:
        raise NotImplementedError("subclass responsibility")

    def _handle_base_data(self) -> None:
        try:
            self._base_data_handler = BaseDataHandler(
                controller_id=self._controller_id,
                rabbitmq_host=self._rabbitmq_host,
                consumers_config=self._consumers_config,
                build_mom_consumer=self._build_mom_base_data_consumer,
                base_data_by_session_id=self._base_data_by_session_id,
                base_data_by_session_id_lock=self._base_data_by_session_id_lock,
                all_base_data_received=self._all_base_data_received,
                all_base_data_received_lock=self._all_base_data_received_lock,
                is_stopped=self.is_stopped,
            )
            self._base_data_handler.run()
        except Exception as e:
            with self._uncaught_exception_lock:
                self._uncaught_exception = e
            self._log_error(f"action: thread_exception | result: fail | error: {e}")
        finally:
            self._set_controller_as_stopped()

    def _handle_stream_data(self) -> None:
        try:
            self._stream_data_handler = StreamDataHandler(
                controller_id=self._controller_id,
                rabbitmq_host=self._rabbitmq_host,
                consumers_config=self._consumers_config,
                producers_config=self._producers_config,
                build_mom_consumer=self._build_mom_stream_data_consumer,
                build_mom_producer=self._build_mom_producer_using,
                base_data_by_session_id=self._base_data_by_session_id,
                base_data_by_session_id_lock=self._base_data_by_session_id_lock,
                all_base_data_received=self._all_base_data_received,
                all_base_data_received_lock=self._all_base_data_received_lock,
                join_key=self._join_key(),
                transform_function=self._transform_function,
                is_stopped=self.is_stopped,
            )
            self._stream_data_handler.run()
        except Exception as e:
            with self._uncaught_exception_lock:
                self._uncaught_exception = e
            self._log_error(f"action: thread_exception | result: fail | error: {e}")
        finally:
            self._set_controller_as_stopped()

    # ============================== PRIVATE - RUN ============================== #

    def _run(self) -> None:
        super()._run()

        self._base_data_thread.start()
        self._stream_data_thread.start()

        self.is_stopped.wait()

    def _close_all(self) -> None:
        super()._close_all()
        self._base_data_thread.join()
        thread_name = self._base_data_thread.name
        self._log_info(f"action: {thread_name}_join | result: success")

        self._stream_data_thread.join()
        thread_name = self._stream_data_thread.name
        self._log_info(f"action: {thread_name}_join | result: success")

        with self._uncaught_exception_lock:
            if self._uncaught_exception is not None:
                raise self._uncaught_exception
