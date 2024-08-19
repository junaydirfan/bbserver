# bbserver

## Bulletin Board Messages and Distributed Agreement

bbserver is a multi-threaded bulletin board server implemented in C. It allows clients to write and read messages while ensuring data consistency across multiple bulletin board instances.

## Purpose

The main objectives of this project are:
- Implement a multi-threaded bulletin board server
- Allow clients to write and read messages
- Synchronize data across multiple server instances
- Provide a practical example of distributed systems and concurrent programming

## Features

- Multi-threaded architecture with thread pool
- Client-server communication via sockets
- Readers-Writers lock for concurrent access to the bulletin board
- Two-phase commit protocol for distributed data synchronization
- Configurable server settings
- Daemon mode support
- Debug logging

## Dependencies

This project relies on standard C libraries:

- stdio.h
- stdlib.h
- unistd.h
- pthread.h
- signal.h
- string.h
- fcntl.h
- sys/types.h
- sys/socket.h
- netinet/in.h
- sys/stat.h
- sys/resource.h
- stdarg.h
- time.h
- semaphore.h
- arpa/inet.h
- stdbool.h

## Installation

1. Clone the repository:

git clone https://github.com/yourusername/bbserver.git

2. Navigate to the project directory:

cd bbserver

3. Compile the server:

make

## Usage

1. Start the server:

./bbserv

Or

./bbserv server.conf

Or with a custom configuration file:

./bbserv <condig_file>.conf

2. Connect to the server using telnet:

telnet localhost 9000

Or:

telnet <ip> <port_number>

3. Send commands:

USER Alice
WRITE Hello World!
READ 1
QUIT

4. To stop all instances of the server:

killall bbserv


## Configuration

The server can be configured using a configuration file (e.g., `server.conf`) with the following parameters:

- BBFILE: Path to the bulletin board file
- BBPORT: Port number for client connections
- SYNCPORT: Port number for peer synchronization
- THMAX: Maximum number of threads in the thread pool
- DAEMON: Flag to run as a daemon process
- DEBUG: Enable debug logging
- DELAY: Introduce artificial delay for testing
- PEER: IP and port of peer servers (can be multiple)

## Key Learning Points

- Implementation of a multi-threaded server with a thread pool
- Synchronization techniques: readers-writers lock and semaphores
- Network programming with sockets in C
- Implementation of a two-phase commit protocol for distributed systems
- Daemon process creation and management
- Configuration management and logging in C programs
- Signal handling for graceful shutdown and reconfiguration

## Conclusion

This project provides practical experience in developing a complex, multi-threaded server application with distributed synchronization. It covers various aspects of systems programming, including network communication, concurrency control, file I/O, and process management in a POSIX environment.
