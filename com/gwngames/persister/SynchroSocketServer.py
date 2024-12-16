import gzip
import json
import socket
import threading
import logging


class SynchroSocketServer:
    logger = logging.getLogger('SynchroSocketServer')

    def __init__(self, host: str, port: int, handler):
        """
        Initialize the server.
        :param host: The host to bind the server to.
        :param port: The port to bind the server to.
        :param handler: A callable to handle received messages. It takes a string (message)
                        and returns a string (response).
        """
        self.host = host
        self.port = port
        self.handler = handler
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.is_running = False
        self.listener_thread = None

    def start(self):
        """Start the server in a separate thread."""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)  # Allow up to 5 pending connections
        self.is_running = True
        self.listener_thread = threading.Thread(target=self._listen_for_connections, daemon=True)
        self.listener_thread.start()
        SynchroSocketServer.logger.info(f"Server started on {self.host}:{self.port}")

    def _listen_for_connections(self):
        """Listen for and accept incoming client connections."""
        while self.is_running:
            try:
                client_socket, client_address = self.server_socket.accept()
                SynchroSocketServer.logger.info(f"Accepted connection from {client_address}")
                threading.Thread(target=self.handle_client, args=(client_socket, client_address), daemon=True).start()
            except Exception as e:
                if self.is_running:  # Log errors only if the server is running
                    SynchroSocketServer.logger.error(f"Error accepting connection: {e}")

    def handle_client(self, client_socket, client_address):
        """Handle communication with a single client."""
        buffer = b''  # Use bytes buffer for raw data
        try:
            while True:
                try:
                    # Receive data from the client
                    chunk = client_socket.recv(1024)
                    if not chunk:
                        # Client has disconnected
                        SynchroSocketServer.logger.info(f"Client {client_address} disconnected")
                        break
                    buffer += chunk

                    # Process complete messages (delimited by '\n')
                    while b'\n' in buffer:
                        # Split the buffer at the first newline
                        message, buffer = buffer.split(b'\n', 1)
                        message = message.strip()

                        if message:
                            SynchroSocketServer.logger.info(f"Received message from {client_address}: {message}")
                            try:
                                # Decode the JSON message
                                decompressed_message = message.decode("utf-8")
                                response = self.handler(decompressed_message)

                                # Compress the response before sending (if needed)
                                # compressed_response = gzip.compress(response.encode('utf-8'))
                                # self.send_message(client_socket, compressed_response)
                            except json.JSONDecodeError as e:
                                SynchroSocketServer.logger.error(f"JSON decoding error from {client_address}: {e}")
                            except Exception as e:
                                SynchroSocketServer.logger.error(f"Error handling message from {client_address}: {e}")
                except Exception as e:
                    SynchroSocketServer.logger.error(f"Error receiving data from {client_address}: {e}")
                    break
        finally:
            # Close the connection and log
            client_socket.close()
            SynchroSocketServer.logger.info(f"Connection with {client_address} closed")

    def send_message(self, client_socket, message: str):
        """Send a message to the client."""
        try:
            full_message = message + '\n'  # Append newline as message delimiter
            client_socket.sendall(full_message.encode())
            SynchroSocketServer.logger.debug(f"Sent message to client: {message}")
        except Exception as e:
            SynchroSocketServer.logger.error(f"Error sending message to client: {e}")

    def stop(self):
        """Stop the server and close all connections."""
        self.is_running = False
        self.server_socket.close()
        SynchroSocketServer.logger.info("Server stopped")
        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join()  # Ensure the listener thread stops properly
