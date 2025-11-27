from .ring_node import RingNode
import logging

class HealthCallbacks:
    def __init__(self, rn: RingNode):
        self._ring_node = rn

    def on_leader_dead(self) -> None:
        current_leader = self._ring_node.election.leader_id
        logging.info(f"Líder {current_leader} caído, iniciando elección...")
        if self._ring_node.election is not None:
            self._ring_node.election.set_leader(None)
        try:
            self._ring_node.election.start_election()
        except Exception as e:
            logging.error(f"Error iniciando elección: {e}")

    def notify_revived_node(self, target_host: str, target_health_port: int) -> None:
        self._ring_node.notify_leadership_to(target_host, target_health_port)
