import ast
import csv
import logging
import asyncio
from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import StreamDataReceived
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
receivedData = []


class StreamingClientProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.buffer = b""  # Buffer to handle stream messages

    def quic_event_received(self, event):
        if isinstance(event, StreamDataReceived):
            if b"\t" in event.data:
                self.close()
                self.transmit()
            self.buffer += event.data
            while b"\n" in self.buffer:
                message, self.buffer = self.buffer.split(b"\n", 1)
                data = message.decode().strip()
                logger.info(f"Received: {data}")
                receivedData.append(ast.literal_eval(data))


async def run_client():
    configuration = QuicConfiguration(is_client=True)
    configuration.verify_mode = False  # For self-signed certificates

    async with connect("127.0.0.1", 4433, configuration=configuration,
                       create_protocol=StreamingClientProtocol) as protocol:
        logger.info("Connected to server!")
        await protocol.wait_closed()

        with open("/Users/bcaserto/Downloads/received_data.csv", 'w', newline='') as csvfile:
            # Create a CSV writer object
            writer = csv.DictWriter(csvfile, fieldnames=receivedData[0].keys())
            # Write the header row
            writer.writeheader()

            # Write the data rows
            for row in receivedData:
                writer.writerow(row)


asyncio.run(run_client())
