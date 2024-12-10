import asyncio
import logging
from aioquic.asyncio import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import HandshakeCompleted, StreamDataReceived
import csv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def csv_to_list_of_dicts(filename):
    with open(filename, 'r') as file:
        csv_reader = csv.DictReader(file)
        return list(csv_reader)


data = csv_to_list_of_dicts('/Users/bcaserto/Downloads/station.csv')
print(data[0])


class StreamingServerProtocol(QuicConnectionProtocol):
    def quic_event_received(self, event):
        if isinstance(event, HandshakeCompleted):
            logger.info("Client handshake completed.")
            self.stream_id = self._quic.get_next_available_stream_id(is_unidirectional=False)
            asyncio.create_task(self.send_stream_data())

    async def send_stream_data(self):
        for i in range(len(data)):  # Send 100 messages
            if not self._quic:
                logger.warning("Connection closed, stopping stream.")
                break
            message = f"{data[i]}\n".encode()
            self._quic.send_stream_data(self.stream_id, message)
            logger.info(f"Sent: {message.decode().strip()}")
            self.transmit()
            #await asyncio.sleep(0.01)

        message = f"\t".encode()
        self._quic.send_stream_data(self.stream_id, message)
        self.transmit()


async def run_server():
    configuration = QuicConfiguration(is_client=False)
    configuration.load_cert_chain(certfile="cert.pem", keyfile="key.pem")

    await serve("127.0.0.1", 4433, configuration=configuration, create_protocol=StreamingServerProtocol)
    logger.info("Server running on 127.0.0.1:4433")
    await asyncio.Event().wait()  # Keep server alive


asyncio.run(run_server())
