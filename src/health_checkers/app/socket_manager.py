from __future__ import annotations

import logging
import socket
import time
from typing import Optional, Callable, Tuple
from dataclasses import dataclass

from .models import Message


@dataclass
class SocketConfig:
    """Configuración para un socket UDP."""
    host: str
    port: int
    timeout_s: float = 1.0
    buffer_size: int = 64 * 1024


class SocketManager:
    """
    Maneja comunicación UDP con retry, timeouts y manejo consistente de errores.
    Abstrae la complejidad de sockets para facilitar testing y reutilización.
    """
    
    def __init__(self, config: SocketConfig, name: str = "socket"):
        self.config = config
        self.name = name
        self._sock: Optional[socket.socket] = None
        self._closed = False
        
        self._initialize_socket()
    
    def _initialize_socket(self) -> None:
        """Inicializa y configura el socket UDP."""
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.bind((self.config.host, self.config.port))
            self._sock.settimeout(self.config.timeout_s)
            self._closed = False
            logging.info(
                f"action: socket_initialized | result: success | "
                f"name: {self.name} | port: {self.config.port}"
            )
        except Exception as e:
            logging.error(
                f"action: socket_initialized | result: fail | "
                f"name: {self.name} | error: {e}"
            )
            raise
    
    def is_valid(self) -> bool:
        """Verifica si el socket es válido y está abierto."""
        try:
            if self._sock is None:
                return False
            if self._closed:
                return False
            if getattr(self._sock, "_closed", False):
                return False
            if self._sock.fileno() == -1:
                return False
            return True
        except Exception:
            return False
    
    def send_message(self, msg: Message, target: Tuple[str, int]) -> bool:
        """
        Envía un mensaje a un destino específico.
        
        Args:
            msg: Mensaje a enviar
            target: Tupla (host, port) del destinatario
            
        Returns:
            True si se envió exitosamente, False en caso contrario
        """
        if not self.is_valid():
            logging.warning(
                f"action: send_message | result: fail | "
                f"name: {self.name} | reason: invalid_socket"
            )
            return False
        
        try:
            data = msg.to_json().encode("utf-8")
            self._sock.sendto(data, target)
            return True
        except OSError as e:
            errno = getattr(e, "errno", None)
            if errno == 9:  # Bad file descriptor - socket cerrado
                self._closed = True
                logging.warning(
                    f"action: send_message | result: fail | "
                    f"name: {self.name} | reason: socket_closed"
                )
                return False
            elif errno == -2:  # Name or service not known
                logging.warning(
                    f"action: send_message | result: fail | "
                    f"name: {self.name} | target: {target} | reason: dns_error"
                )
                return False
            else:
                logging.error(
                    f"action: send_message | result: fail | "
                    f"name: {self.name} | target: {target} | error: {e}"
                )
                return False
        except Exception as e:
            logging.error(
                f"action: send_message | result: fail | "
                f"name: {self.name} | target: {target} | error: {e}"
            )
            return False
    
    def receive_message(self, timeout_s: Optional[float] = None) -> Optional[Tuple[Message, Tuple[str, int]]]:
        """
        Recibe un mensaje del socket.
        
        Args:
            timeout_s: Timeout específico (None usa el timeout del socket)
            
        Returns:
            Tupla (mensaje, dirección) o None si timeout/error
        """
        if not self.is_valid():
            return None
        
        original_timeout = None
        if timeout_s is not None:
            try:
                original_timeout = self._sock.gettimeout()
                self._sock.settimeout(timeout_s)
            except OSError as e:
                if getattr(e, "errno", None) == 9:
                    self._closed = True
                    return None
        
        try:
            data, addr = self._sock.recvfrom(self.config.buffer_size)
            msg = Message.from_json(data.decode("utf-8"))
            return (msg, addr)
        except socket.timeout:
            return None
        except OSError as e:
            if getattr(e, "errno", None) == 9:
                self._closed = True
            return None
        except Exception as e:
            logging.error(
                f"action: receive_message | result: fail | "
                f"name: {self.name} | error: {e}"
            )
            return None
        finally:
            if original_timeout is not None:
                try:
                    self._sock.settimeout(original_timeout)
                except Exception:
                    pass
    
    def send_with_ack(
        self,
        msg: Message,
        target: Tuple[str, int],
        expected_ack_kind: str,
        timeout_s: float,
        max_retries: int = 1,
        retry_delay_s: float = 1.0,
        ack_filter: Optional[Callable[[Message], bool]] = None
    ) -> bool:
        """
        Envía un mensaje y espera un ACK específico con retry.
        
        Args:
            msg: Mensaje a enviar
            target: Tupla (host, port) del destinatario
            expected_ack_kind: Tipo de mensaje ACK esperado
            timeout_s: Timeout para esperar el ACK
            max_retries: Número máximo de intentos
            retry_delay_s: Delay entre reintentos
            ack_filter: Función opcional para filtrar ACKs válidos
            
        Returns:
            True si recibió el ACK esperado, False en caso contrario
        """
        for attempt in range(1, max_retries + 1):
            # Enviar mensaje
            if not self.send_message(msg, target):
                if attempt < max_retries:
                    logging.info(
                        f"action: send_with_ack | status: retrying | "
                        f"name: {self.name} | attempt: {attempt}/{max_retries}"
                    )
                    time.sleep(retry_delay_s)
                    continue
                return False
            
            # Esperar ACK
            start = time.monotonic()
            while time.monotonic() - start < timeout_s:
                result = self.receive_message(timeout_s=timeout_s)
                
                if result is None:
                    break  # Timeout o error
                
                ack_msg, ack_addr = result
                
                if ack_msg.kind == expected_ack_kind:
                    # Aplicar filtro adicional si existe
                    if ack_filter is None or ack_filter(ack_msg):
                        logging.info(
                            f"action: send_with_ack | result: success | "
                            f"name: {self.name} | ack_kind: {expected_ack_kind} | "
                            f"attempt: {attempt}/{max_retries}"
                        )
                        return True
            
            # No recibió ACK válido en este intento
            if attempt < max_retries:
                logging.info(
                    f"action: send_with_ack | status: no_ack | "
                    f"name: {self.name} | retrying: {attempt}/{max_retries}"
                )
                time.sleep(retry_delay_s)
        
        logging.warning(
            f"action: send_with_ack | result: fail | "
            f"name: {self.name} | ack_kind: {expected_ack_kind} | "
            f"attempts_exhausted: {max_retries}"
        )
        return False
    
    def set_timeout(self, timeout_s: float) -> bool:
        """
        Cambia el timeout del socket.
        
        Args:
            timeout_s: Nuevo timeout en segundos
            
        Returns:
            True si se cambió exitosamente, False en caso contrario
        """
        if not self.is_valid():
            return False
        
        try:
            self._sock.settimeout(timeout_s)
            self.config.timeout_s = timeout_s
            return True
        except OSError as e:
            if getattr(e, "errno", None) == 9:
                self._closed = True
            return False
        except Exception:
            return False
    
    def close(self) -> None:
        """Cierra el socket de forma segura."""
        if self._closed or self._sock is None:
            return
        
        try:
            if self.is_valid():
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    self._sock.close()
                except Exception:
                    pass
            self._closed = True
            logging.info(f"action: close_socket | result: success | name: {self.name}")
        except Exception as e:
            try:
                self._sock.close()
            except Exception:
                pass
            self._closed = True
            logging.error(
                f"action: close_socket | result: fail | "
                f"name: {self.name} | error: {e}"
            )
    
    def __enter__(self):
        """Context manager support."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support."""
        self.close()
        return False
