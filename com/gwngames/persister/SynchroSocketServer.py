import contextlib
import logging
import socket
import threading
import time
from typing import Callable

from com.gwngames.persister.Context import Context


class SynchroSocketServer:
    logger = logging.getLogger('SynchroSocketServer')

    def __init__(self, host: str, port: int, handler: Callable[[str], str]):
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
        self.ctx = Context()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.is_running = False
        self.listener_thread = None
        self.connections = {}  # Track active connections and their last activity time
        self.connection_lock = threading.Lock()  # Protect access to the connections dictionary

    def start(self):
        """Start the server in a separate thread."""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(self.ctx.get_config().get_value("max_connections"))
        self.is_running = True

        # Start listener and connection monitor threads
        self.listener_thread = threading.Thread(target=self._listen_for_connections, daemon=True)
        self.listener_thread.start()
        threading.Thread(target=self._monitor_connections, daemon=True).start()

        SynchroSocketServer.logger.info(f"Server started on {self.host}:{self.port}")

    def _listen_for_connections(self):
        """Listen for and accept incoming client connections."""
        while self.is_running:
            try:
                client_socket, client_address = self.server_socket.accept()
                with self.connection_lock:
                    self.connections[client_socket] = time.time()  # Track the last activity
                SynchroSocketServer.logger.info(f"Accepted connection from {client_address}")
                threading.Thread(target=self.handle_client, args=(client_socket, client_address), daemon=True).start()
            except Exception as e:
                if self.is_running:
                    SynchroSocketServer.logger.error(f"Error accepting connection: {e}")

    def _monitor_connections(self):
        """Monitor connections and close inactive ones."""
        max_unactive_connection_time = self.ctx.get_config().get_value("max_unactive_connection_seconds")
        unactive_conn_listen_seconds = self.ctx.get_config().get_value("unactive_conn_listen_seconds")
        while self.is_running:
            current_time = time.time()
            with self.connection_lock:
                to_close = [
                    sock for sock, last_active in self.connections.items()
                    if current_time - last_active > int(max_unactive_connection_time)  # X seconds inactivity
                ]
            for sock in to_close:
                try:
                    SynchroSocketServer.logger.info(f"Closing inactive connection: {sock}")
                    self._remove_connection(sock, None)
                except Exception as e:
                    SynchroSocketServer.logger.error(f"Error closing inactive connection: {e}")
            time.sleep(unactive_conn_listen_seconds)  # Check every X seconds

    def handle_client(self, client_socket, client_address):
        """Handle communication with a single client."""
        with contextlib.closing(client_socket):
            buffer = b''  # Use bytes buffer for raw data
            client_socket.settimeout(20 * 60)  # Set a timeout of 20 minutes (1200 seconds)
            try:
                while True:
                    try:
                        chunk = client_socket.recv(1024)
                        if not chunk:
                            break
                        buffer += chunk

                        with self.connection_lock:
                            self.connections[client_socket] = time.time()  # Update last activity

                        # Process complete messages (delimited by '\n')
                        while b'\n' in buffer:
                            message, buffer = buffer.split(b'\n', 1)
                            message = message.strip()

                            if message:
                                try:
                                    decompressed_message = message.decode("utf-8")
                                    SynchroSocketServer.logger.info(
                                        f"Received message from {client_address}")

                                    response = self.handler(decompressed_message)
                                    #self.send_message(client_socket, response)
                                except Exception as e:
                                    SynchroSocketServer.logger.error(f"Error handling message from {client_address}: {e}")
                    except socket.timeout:
                        SynchroSocketServer.logger.info(f"Connection with {client_address} timed out due to inactivity")
                        break
                    except OSError as e:
                        SynchroSocketServer.logger.error(f"Error receiving data from {client_address}: {e}")
                        break
            finally:
                self._remove_connection(client_socket, client_address)

    def _remove_connection(self, client_socket, client_address):
        """Remove and close the connection."""
        with self.connection_lock:
            if client_socket in self.connections:
                SynchroSocketServer.logger.debug(f"Removing connection for {client_address}")
                del self.connections[client_socket]
        try:
            client_socket.shutdown(socket.SHUT_RDWR)
        except Exception as e:
            SynchroSocketServer.logger.error(f"Error shutting down socket: {e}")
        try:
            client_socket.close()
            SynchroSocketServer.logger.debug(f"Closed socket for {client_address}")
        except Exception as e:
            SynchroSocketServer.logger.error(f"Error closing socket for {client_address}: {e}")
        if client_address:
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
        with self.connection_lock:
            for sock in list(self.connections.keys()):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except Exception as e:
                    SynchroSocketServer.logger.error(f"Error shutting down socket during shutdown: {e}")
                try:
                    sock.close()
                except Exception as e:
                    SynchroSocketServer.logger.error(f"Error closing socket during shutdown: {e}")
            self.connections.clear()
        SynchroSocketServer.logger.info("Server stopped")
        if self.listener_thread and self.listener_thread.is_alive():
            self.listener_thread.join()