# Compiler and Flags
CC = gcc
CFLAGS = -Wall -Wextra -pthread

# Target Executable (remains bbserv)
TARGET = bbserv

# Source Files 
SRCS = server.c

# Object Files (generated from source files)
OBJS = $(SRCS:.c=.o)

# Default Target: Build the server
all: $(TARGET)

# Build the server
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

# Compile source files into object files
%.o: %.c
	$(CC) $(CFLAGS) -c $<

# Clean Target: Remove generated files
clean:
	rm -f $(TARGET) $(OBJS) *.pid bbserv.log temp.txt

# Daemon Target: Start the server as a daemon
daemon: $(TARGET)
	./$(TARGET) &

# Kill Target: Stop the daemon (if running)
kill:
	kill -TERM `cat bbserv.pid`

# Run Target: Run the server in the foreground
run: $(TARGET)
	./$(TARGET)

# Logging Target: Check recent syslog entries
logs:
	tail -n 20 /var/log/syslog | grep bbserv

# Help Target: Display available commands
help:
	@echo "Available targets:"
	@echo "  all     - Build the server"
	@echo "  daemon  - Start the server as a daemon"
	@echo "  kill    - Stop the daemon (if running)"
	@echo "  run     - Run the server in the foreground"
	@echo "  clean   - Remove generated files"
	@echo "  logs    - Show recent syslog entries"
	@echo "  help    - Display this help message"

# Phony Targets (always run, regardless of file existence)
.PHONY: all clean daemon kill run logs help
