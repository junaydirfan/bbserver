#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <stdarg.h>
#include <time.h>
#include <semaphore.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <stdbool.h>
#include <netdb.h>

#define BUFFER_SIZE 1024
#define MAX_PEERS 10

typedef struct {
    void *(*task)(void *);
    void *arg;
} thread_task_t;

typedef struct {
    int socket;
    char username[BUFFER_SIZE];
} client_data_t;

typedef struct {
    thread_task_t *tasks;
    int task_capacity;
    int task_count;
    int thread_count;
    pthread_t *threads;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    int is_shutdown;
} thread_pool_t;

// Readers-writer lock structure
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t read;
    pthread_cond_t write;
    int readers;
    int writers;
    int waiting_writers;

    // Rollback mechanism:
    int last_message_number;  // Store the last message number modified
    char rollback_buffer[BUFFER_SIZE]; // Buffer to hold the original message
} rwlock_t;

// Structure to represent a peer
typedef struct {
    char ip[INET_ADDRSTRLEN];
    int port;
} Peer;

// Structure to represent the PeerNetwork
typedef struct {
    int port;
    Peer peers[MAX_PEERS];
    int peer_count;
    pthread_t listener_thread;
    pthread_mutex_t mutex;
    bool running;
} PeerNetwork;

// Global variables for server configuration
int bp, sp, T, d, D, Y;
char bbfile[BUFFER_SIZE]; // Bulletin board file name
int server_socket; // Global variable to store the server socket
volatile sig_atomic_t running = 1; // Flag for signal handler
volatile sig_atomic_t restart_server = 0;
char username[BUFFER_SIZE] = "nobody"; // Default usernames
volatile sig_atomic_t rollback_after_hup = 0;
FILE *log_file = NULL;
char original_dir[BUFFER_SIZE];
pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;
sem_t connection_semaphore;
char peers[MAX_PEERS][BUFFER_SIZE];
int peer_count = 0;
const char *config_file = "server.conf";
PeerNetwork manager;
rwlock_t lock;

// Function prototypes
void *client_handler(void *socket_desc);
void init_rwlock(rwlock_t *lock);
void read_lock(rwlock_t *lock);
void read_unlock(rwlock_t *lock);
void write_lock(rwlock_t *lock);
void write_unlock(rwlock_t *lock);
void signal_handler(int sig);
void handle_user_command(int client_sock, char *username_arg, client_data_t *client_data);
void handle_read_command(int client_sock, int message_number);
void handle_write_command(int client_sock, char *message, char *username);
void handle_replace_command(int client_sock, int message_number, char *message, char *username);
void handle_quit_command(int client_sock);
void read_config(const char *filename);
void daemonize();
void debug_log(const char *format, ...);
void rollback_last_message();
void log_message(const char *format, ...);
thread_pool_t *thread_pool;
thread_pool_t *thread_pool_create(int thread_count);
void thread_pool_destroy(thread_pool_t *pool);
void *thread_pool_worker(void *arg);
void create_pid_file();
void remove_pid_file();
void filter_non_printable(char *str);
void* listener(void* arg);
bool sync_peer_network(PeerNetwork* manager, const char* operation, const char* data);
int create_socket();
bool send_message(int sockfd, const char* message);
char* receive_message(int sockfd);
void handle_connection(int clientSock, PeerNetwork* manager);
void init_peer_network(PeerNetwork* manager, int port, Peer peers[], int peer_count);
void start_listener(PeerNetwork* manager);
void handle_connection(int client_sock, PeerNetwork* manager);
bool parse_peer(const char* peer_string, char* ip, int* port);

int main(int argc, char *argv[]) {
    log_file = fopen("bbserv.log", "a");
    if (argc > 1) {
        config_file = argv[1];
    }

    read_config(config_file);
    
    // Validates peers and converts them to Peers array  
    Peer peers_struct[MAX_PEERS];
    int valid_peer_count = 0;
    for (int i = 0; i < peer_count; i++) {
        char ip[INET_ADDRSTRLEN];
        int port;
        if (parse_peer(peers[i], ip, &port)) {
            strncpy(peers_struct[valid_peer_count].ip, ip, INET_ADDRSTRLEN);
            peers_struct[valid_peer_count].ip[INET_ADDRSTRLEN - 1] = '\0';
            peers_struct[valid_peer_count].port = port;
            valid_peer_count++;
        } else {
            log_message("Skipping invalid peer: %s", peers[i]);
        }
    }

    init_peer_network(&manager, sp, peers_struct, valid_peer_count);
    start_listener(&manager);

    thread_pool = thread_pool_create(T);
    if (thread_pool == NULL) {
        log_message("ERROR: Failed to create thread pool");
        exit(EXIT_FAILURE);
    }

    if (getcwd(original_dir, sizeof(original_dir)) == NULL) {
        perror("ERROR: Can't get current working directory");
        exit(EXIT_FAILURE);
    }

    if(D){
       // Print configuration for debugging purposes
        printf("Configuration:\n");
        printf("THMAX: %d\n", T);
        printf("BBPORT: %d\n", bp);
        printf("SYNCPORT: %d\n", sp);
        printf("BBFILE: %s\n", bbfile);
        printf("DAEMON: %d\n", d);
        printf("DEBUG: %d\n", D);
        printf("DELAY: %d\n", Y);
        printf("PEERS: ");
        for (int i = 0; i < peer_count; i++) {
            printf("%s ", peers[i]);
        }
        printf("\n"); 
    }

    if (strlen(bbfile) == 0) {
        fprintf(stderr, "Error: bbfile path not set in configuration\n");
        exit(EXIT_FAILURE);
    }

    // Signal handling
    signal(SIGINT, signal_handler);
    signal(SIGQUIT, signal_handler);
    signal(SIGHUP, signal_handler);

    // Initialize the readers-writer lock
    init_rwlock(&lock);

    // Create socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) {
        perror("Could not create socket");
        exit(EXIT_FAILURE);
    }

    int option = 1;
    setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option)); // Ensure we can reuse the address

    // Prepare the sockaddr_in structure
    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(bp);

    // Bind
    if (bind(server_socket, (struct sockaddr *)&server, sizeof(server)) < 0) {
        perror("Bind failed");
        close(server_socket);
        exit(EXIT_FAILURE);
    }
    log_message("INFO: Bind done on port %d", bp);

    // Listen
    if (listen(server_socket, 3) < 0) {
        perror("Listen failed");
        close(server_socket);
        exit(EXIT_FAILURE);
    }

    // Daemonize if needed
    if (d) {
        daemonize();
    } else {
        if (log_file == NULL) {
            perror("Failed to open log file");
            exit(EXIT_FAILURE);
        }
    }

    // Accept incoming connections
    log_message("INFO: Entering accept loop");
    puts("Waiting for incoming connections...");

    int c = sizeof(struct sockaddr_in);
    struct sockaddr_in client;
    int client_socket;

    if (sem_init(&connection_semaphore, 0, T) != 0) {
        perror("Semaphore initialization failed");
        exit(EXIT_FAILURE);
    }

    while(running){
        client_socket = accept(server_socket, (struct sockaddr *)&client, (socklen_t*)&c);
        if (client_socket < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Timeout occurred, continue the loop
                continue;
            }
            continue;
        }
        // Set socket to blocking mode
        int flags = fcntl(client_socket, F_GETFL, 0);
        fcntl(client_socket, F_SETFL, flags & ~O_NONBLOCK);


        debug_log("Accepted connection from %s:%d", inet_ntoa(client.sin_addr), ntohs(client.sin_port));

        // Try to acquire a semaphore slot
        if (sem_trywait(&connection_semaphore) != 0) {
            char *msg = "Server is at full capacity. Please try again later.\n";
            send(client_socket, msg, strlen(msg), 0);
            close(client_socket);
            log_message("INFO: Connection rejected - Server at full capacity");
            continue;
        }
        
        log_message("INFO: Connection accepted");

        // Allocate new client_data_t structure
        client_data_t *client_data = malloc(sizeof(client_data_t));
        if (client_data == NULL) {
            perror("Failed to allocate memory for client data");
            close(client_socket);
            sem_post(&connection_semaphore);
            continue;
        }

        // Initialize client_data
        client_data->socket = client_socket;
        strcpy(client_data->username, "nobody"); // Default username

        // Create thread for new client
        pthread_t thread_id;
        if (pthread_create(&thread_id, NULL, client_handler, (void*)client_data) != 0) {
            perror("Could not create thread");
            free(client_data);
            close(client_socket);
            sem_post(&connection_semaphore);
            continue;
        }

        pthread_detach(thread_id);
        debug_log("Created thread for handling client %s:%d", inet_ntoa(client.sin_addr), ntohs(client.sin_port));
    }

    log_message("INFO: Exiting accept loop");
    
    if (client_socket < 0) {
        close(server_socket);
        exit(EXIT_FAILURE);
    }
    if (log_file) {
        fclose(log_file);
    }
    close(server_socket);
    thread_pool_destroy(thread_pool);
    sem_destroy(&connection_semaphore);
    return 0;
}

thread_pool_t *thread_pool_create(int thread_count) {
    thread_pool_t *pool = malloc(sizeof(thread_pool_t));
    pool->task_capacity = 100;  // Adjust as needed
    pool->tasks = malloc(sizeof(thread_task_t) * pool->task_capacity);
    pool->task_count = 0;
    pool->thread_count = thread_count;
    pool->threads = malloc(sizeof(pthread_t) * thread_count);
    pthread_mutex_init(&pool->lock, NULL);
    pthread_cond_init(&pool->not_empty, NULL);
    pthread_cond_init(&pool->not_full, NULL);
    pool->is_shutdown = 0;

    for (int i = 0; i < thread_count; i++) {
        pthread_create(&pool->threads[i], NULL, thread_pool_worker, pool);
    }

    return pool;
}

void *thread_pool_worker(void *arg) {
    thread_pool_t *pool = (thread_pool_t *)arg;
    while (1) {
        pthread_mutex_lock(&pool->lock);
        while (pool->task_count == 0 && !pool->is_shutdown) {
            pthread_cond_wait(&pool->not_empty, &pool->lock);
        }
        if (pool->is_shutdown) {
            pthread_mutex_unlock(&pool->lock);
            pthread_exit(NULL);
        }
        thread_task_t task = pool->tasks[0];
        for (int i = 0; i < pool->task_count - 1; i++) {
            pool->tasks[i] = pool->tasks[i + 1];
        }
        pool->task_count--;
        pthread_mutex_unlock(&pool->lock);
        pthread_cond_signal(&pool->not_full);
        (*(task.task))(task.arg);
    }
    return NULL;
}

void thread_pool_destroy(thread_pool_t *pool) {
    if (pool == NULL) return;

    pthread_mutex_lock(&pool->lock);
    pool->is_shutdown = 1;
    pthread_cond_broadcast(&pool->not_empty);
    pthread_mutex_unlock(&pool->lock);

    for (int i = 0; i < pool->thread_count; i++) {
        pthread_join(pool->threads[i], NULL);
    }

    free(pool->tasks);
    free(pool->threads);
    pthread_mutex_destroy(&pool->lock);
    pthread_cond_destroy(&pool->not_empty);
    pthread_cond_destroy(&pool->not_full);
    free(pool);
}

void *client_handler(void *socket_desc) {
    client_data_t *client_data = (client_data_t*)socket_desc;
    int sock = client_data->socket;
    int client_sock = sock;
    char buffer[BUFFER_SIZE];
    int n;

    // Send greeting message
    char *greeting = "0.0 Welcome to the Bulletin Board Server\n";
    send(sock, greeting, strlen(greeting), 0);

    while (running) {
        n = recv(sock, buffer, BUFFER_SIZE - 1, 0);
        if (n > 0) {
            buffer[n] = '\0';
            filter_non_printable(buffer);

            if (strncmp(buffer, "USER", 4) == 0) {
                handle_user_command(sock, buffer + 5, client_data);
            } else if (strncmp(buffer, "READ", 4) == 0) {
                int message_number = atoi(buffer + 5);
                handle_read_command(sock, message_number);
            } else if (strncmp(buffer, "WRITE", 5) == 0) {
                char commandSync[1000];
                char *message = buffer + 6;
                strncpy(commandSync, client_data->username, sizeof(commandSync) - 1);
                commandSync[sizeof(commandSync) - 1] = '\0';

                size_t remaining = sizeof(commandSync) - strlen(commandSync) - 1;
                strncat(commandSync, "/", remaining);
                strncat(commandSync, message, remaining - 1);
                commandSync[sizeof(commandSync) - 1] = '\0';
                
                if(peer_count > 0){
                    sync_peer_network(&manager, "WRITE", commandSync);
                }
                
                // Write locally regardless of sync success
                handle_write_command(sock, message, client_data->username);
                
            } else if (strncmp(buffer, "REPLACE", 7) == 0) {
                int message_number;
                char *message = strchr(buffer + 8, '/');
                if (message) {
                    *message = '\0';
                    message++;
                    message_number = atoi(buffer + 8);
                    char sync_data[BUFFER_SIZE];
                    snprintf(sync_data, BUFFER_SIZE, "%d %s", message_number, message);

                    if(peer_count > 0){
                        sync_peer_network(&manager, "REPLACE", sync_data);
                    }
                    
                    handle_replace_command(sock, message_number, message, client_data->username);
                }
            } else if (strncmp(buffer, "QUIT", 4) == 0) {
                handle_quit_command(sock);
                break;
            } else if (strncmp(buffer, "ROLLBACK", 8) == 0) {
                write_lock(&lock);
                FILE *file = fopen(bbfile, "r");
                if (lock.last_message_number == -1) {
                    char *response = "ROLLBACK FAILED: No previous message to roll back\n";
                    send(client_sock, response, strlen(response), 0);
                }

                if (file != NULL) {
                    char line[BUFFER_SIZE];
                    while (fgets(line, sizeof(line), file)) {
                        int num;
                        char poster[BUFFER_SIZE];
                        sscanf(line, "%d/%[^/]/%*s", &num, poster);
                        if (num == lock.last_message_number && strcmp(poster, client_data->username) == 0) {
                            rollback_last_message();
                            char *response = "ROLLBACK OK\n";
                            send(client_sock, response, strlen(response), 0);
                            break;
                        }
                    }
                    fclose(file);
                }
                write_unlock(&lock);
            } else {
                char *error_msg = "ERROR Invalid command\n";
                send(sock, error_msg, strlen(error_msg), 0);
            }
        } else if (n == 0) {
            debug_log("Client disconnected");
            break;
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No data available, try again later
                continue;
            } else {
                perror("Error receiving data");
                break;
            }
        }
    }

    // Cleanup
    debug_log("Closing connection for client %d", sock);
    close(sock);
    free(client_data);
    sem_post(&connection_semaphore);
    return NULL;
}

void init_rwlock(rwlock_t *lock) {
    pthread_mutex_init(&lock->mutex, NULL);
    pthread_cond_init(&lock->read, NULL);
    pthread_cond_init(&lock->write, NULL);
    lock->readers = 0;
    lock->writers = 0;
    lock->waiting_writers = 0;
}

void read_lock(rwlock_t *lock) {
    pthread_mutex_lock(&lock->mutex);
    while (lock->writers > 0 || lock->waiting_writers > 0) {
        pthread_cond_wait(&lock->read, &lock->mutex);
    }
    lock->readers++;
    pthread_mutex_unlock(&lock->mutex);

    if (Y) {
        log_message("DEBUG: Starting read operation");
        sleep(3);  // 3 second delay for read operations
    }
}

void read_unlock(rwlock_t *lock) {
    if (Y) {
        log_message("DEBUG: Ending read operation\n");
    }
    pthread_mutex_lock(&lock->mutex);
    lock->readers--;
    if (lock->readers == 0 && lock->waiting_writers > 0) {
        pthread_cond_signal(&lock->write);
    }
    pthread_mutex_unlock(&lock->mutex);
}

void write_lock(rwlock_t *lock) {
    pthread_mutex_lock(&lock->mutex);
    lock->waiting_writers++;
    while (lock->writers > 0 || lock->readers > 0) {
        pthread_cond_wait(&lock->write, &lock->mutex);
    }
    lock->waiting_writers--;
    lock->writers = 1;
    pthread_mutex_unlock(&lock->mutex);

    if (Y) {
        log_message("DEBUG: Starting write operation");
        sleep(6);  // 6 second delay for write operations
    }
}

void write_unlock(rwlock_t *lock) {
    if (Y) {
        log_message("DEBUG: Ending write operation\n");
    }
    pthread_mutex_lock(&lock->mutex);
    lock->writers = 0;
    if (lock->waiting_writers > 0) {
        pthread_cond_signal(&lock->write);
    } else {
        pthread_cond_broadcast(&lock->read);
    }
    pthread_mutex_unlock(&lock->mutex);
}

void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGQUIT) {
        log_message("INFO: Server shutdown!");
        running = 0; // Set flag to stop server
        rollback_after_hup = 1; // Mark for rollback if server is shutting down
        // Closing sockets and terminate threads
        // Graceful Shutdown
        shutdown(server_socket, SHUT_RDWR); // Stop accepting new connections
        remove_pid_file(); //Removing PID file
        close(server_socket); // Close the server socket after shutdown
    } else if (sig == SIGHUP) {
        // Reread configuration file and restart operations here
        read_config(config_file);
        if(rollback_after_hup){
            rollback_last_message();
            rollback_after_hup=0; //Reset the flag
        }
    }
}

void handle_user_command(int client_sock, char *username_arg, client_data_t *client_data) {
    debug_log("Handling USER command for client %d", client_sock);
    char response[BUFFER_SIZE];
    int n = snprintf(response, BUFFER_SIZE, "1.0 HELLO %s\n", username_arg);
    if (n < 0 || n >= BUFFER_SIZE) {
        char *error_msg = "1.2 ERROR USER Invalid username\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        debug_log("Invalid username provided by client %d", client_sock);
    } else {
        username_arg[strcspn(username_arg, "\r\n")] = 0;
        strncpy(client_data->username, username_arg, BUFFER_SIZE - 1);
        client_data->username[BUFFER_SIZE - 1] = '\0'; // Ensure null-termination
        send(client_sock, response, strlen(response), 0);
        debug_log("Set username to %s for client %d", username_arg, client_sock);
    }
}

void handle_read_command(int client_sock, int message_number) {
    debug_log("Handling READ command for client %d", client_sock);
    FILE *file;
    char line[BUFFER_SIZE];
    int found = 0;

    read_lock(&lock);
    file = fopen(bbfile, "r");
    if (file == NULL) {
        char error_msg[BUFFER_SIZE];
        snprintf(error_msg, BUFFER_SIZE, "2.2 ERROR READ Could not open file: %s\n", strerror(errno));
        send(client_sock, error_msg, strlen(error_msg), 0);
        log_message("ERROR: Failed to open bulletin board file: %s", strerror(errno));
        read_unlock(&lock);
        return;
    }

    while (fgets(line, sizeof(line), file)) {
        int num;
        char poster[BUFFER_SIZE], message[BUFFER_SIZE];
        sscanf(line, "%d/%[^/]/%[^\n]", &num, poster, message);
        if (num == message_number) {
            char response[BUFFER_SIZE];
            int n = snprintf(response, BUFFER_SIZE, "2.0 MESSAGE %d %s/%s\n", num, poster, message);
            if (n < 0 || n >= BUFFER_SIZE) {
                char *error_msg = "2.2 ERROR READ Message too long\n";
                send(client_sock, error_msg, strlen(error_msg), 0);
            } else {
                send(client_sock, response, strlen(response), 0);
            }
            found = 1;
            break;
        }
    }

    if (!found) {
        char response[BUFFER_SIZE];
        int n = snprintf(response, BUFFER_SIZE, "2.1 UNKNOWN %d Message not found\n", message_number);
        if (n < 0 || n >= BUFFER_SIZE) {
            char *error_msg = "2.2 ERROR READ Message not found\n";
            send(client_sock, error_msg, strlen(error_msg), 0);
        } else {
            send(client_sock, response, strlen(response), 0);
        }
    }

    if (ferror(file)) {
        log_message("ERROR: File read error occurred: %s", strerror(errno));
    }

    fclose(file);
    read_unlock(&lock);
}

void handle_write_command(int client_sock, char *message, char *username) {
    debug_log("Handling WRITE command for client %d", client_sock);
    FILE *file;
    int message_number = 1;
    char line[BUFFER_SIZE];

    // Store original message for potential rollback
    write_lock(&lock);
    debug_log("Acquired write lock for WRITE command");
    log_message("INFO: Trying to open file: %s", bbfile);
    file = fopen(bbfile, "a+");
    if (file == NULL) {
        char *error_msg = "3.2 ERROR WRITE Could not open file\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        write_unlock(&lock);
        debug_log("Failed to open bulletin board file for writing");
        return;
    }

    lock.last_message_number = message_number;
    strcpy(lock.rollback_buffer, ""); // Clear the buffer before use

    // Read and store the original line if replacing an existing message
    if (message_number != 1) {  // Assuming message numbers start at 1
        FILE *readFile = fopen(bbfile, "r");
        if (readFile != NULL) {
            char line[BUFFER_SIZE];
            while (fgets(line, sizeof(line), readFile)) {
                int num;
                sscanf(line, "%d/%*[^/]/%*s", &num);
                if (num == message_number) {
                    strcpy(lock.rollback_buffer, line);
                    break;
                }
            }
            fclose(readFile);
        }
    } else {
        lock.last_message_number = -1; // No message to rollback
    }

    while (fgets(line, sizeof(line), file)) {
        int num;
        sscanf(line, "%d/%*[^/]/%*s", &num);
        if (num >= message_number) {
            message_number = num + 1;
        }
    }

    // Remove trailing newline or carriage return characters from the message
    message[strcspn(message, "\r\n")] = 0;

    fprintf(file, "%d/%s/%s\n", message_number, username, message);
    fclose(file);
    write_unlock(&lock);
    debug_log("Released write lock for WRITE command");

    char response[BUFFER_SIZE];
    int n = snprintf(response, BUFFER_SIZE, "3.0 WROTE %d\n", message_number);
    if (n < 0 || n >= BUFFER_SIZE) {
        char *error_msg = "3.2 ERROR WRITE Could not write message\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        debug_log("Failed to send write confirmation to client %d", client_sock);
    } else {
        send(client_sock, response, strlen(response), 0);
        debug_log("Successfully wrote message %d for client %d", message_number, client_sock);
    }
}

void filter_non_printable(char *str) {
    char *src = str, *dst = str;
    while (*src) {
        if (isprint((unsigned char)*src)) {
            *dst++ = *src;
        }
        src++;
    }
    *dst = '\0';
}


void create_pid_file() {
    char pid_file_path[BUFFER_SIZE * 2];
    snprintf(pid_file_path, sizeof(pid_file_path), "%s/bbserv.pid", original_dir);
    FILE *pid_file = fopen(pid_file_path, "w");
    if (pid_file == NULL) {
        log_message("ERROR: Failed to open PID file: %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
    fprintf(pid_file, "%d\n", getpid());
    fclose(pid_file);
}

void remove_pid_file() {
    char pid_file_path[BUFFER_SIZE * 2];
    snprintf(pid_file_path, sizeof(pid_file_path), "%s/bbserv.pid", original_dir);
    unlink(pid_file_path);
}

void handle_replace_command(int client_sock, int message_number, char *message, char *username) {
    FILE *file, *tempFile;
    char line[BUFFER_SIZE];
    int found = 0;
    
    write_lock(&lock);

    file = fopen(bbfile, "r");
    tempFile = tmpfile();
    
    if (file == NULL || tempFile == NULL) {
        char *error_msg = "3.2 ERROR WRITE Could not open file\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        write_unlock(&lock);
        return;
    }

    while (fgets(line, sizeof(line), file)) {
        int num;
        char poster[BUFFER_SIZE];
        sscanf(line, "%d/%[^/]/%*s", &num, poster);
        
        if (num == message_number) {
            found = 1;
            fprintf(tempFile, "%d/%s/%s\n", message_number, username, message);
            strcpy(lock.rollback_buffer, line);
            lock.last_message_number = message_number;
        } else {
            fputs(line, tempFile);
        }
        fflush(tempFile);
    }

    fclose(file);

    if (found) {
        rewind(tempFile);
        file = fopen(bbfile, "w");
        
        while (fgets(line, sizeof(line), tempFile)) {
            fputs(line, file);
            fflush(file);
        }
        
        fclose(file);
        char response[BUFFER_SIZE];
        snprintf(response, BUFFER_SIZE, "3.0 WROTE %d\n", message_number);
        send(client_sock, response, strlen(response), 0);
    } else {
        char response[BUFFER_SIZE];
        snprintf(response, BUFFER_SIZE, "3.1 UNKNOWN %d Message not found\n", message_number);
        send(client_sock, response, strlen(response), 0);
    }

    fclose(tempFile);
    write_unlock(&lock);
}

void rollback_last_message() {
    if (lock.last_message_number == -1) {
        return; // No message to roll back
    }

    write_lock(&lock);
    FILE *file = fopen(bbfile, "r+");
    if (file == NULL) {
        log_message("ERROR: Rollback failed: Could not open bulletin board file for rollback"); // Log error
        return; 
    }
    FILE *tempFile = fopen("temp.txt", "w");
    if (tempFile == NULL) {
        log_message("ERROR: Rollback failed: Could not create temporary file for rollback"); // Log error
        fclose(file);
        return;
    }
    struct stat st;
    if (stat(bbfile, &st) == 0 && st.st_size == 0) { 
        // Handle empty file case (e.g., log a warning or reset lock.last_message_number)
        log_message("ERROR: Rollback attempted on an empty bulletin board file.");
        lock.last_message_number = -1; // Reset since there is nothing to rollback
        return;
    }


    if (file != NULL) {
        if (tempFile != NULL) {
            char line[BUFFER_SIZE];
            while (fgets(line, sizeof(line), file)) {
                int num;
                sscanf(line, "%d/%*[^/]/%*s", &num);
                if (num == lock.last_message_number) {
                    fputs(lock.rollback_buffer, tempFile); // Restore original message
                } else {
                    fputs(line, tempFile);
                }
            }

            fclose(tempFile);
            fclose(file);
            remove(bbfile);
            rename("temp.txt", bbfile);
        }
    }
    lock.last_message_number = -1; // Reset after rollback
    write_unlock(&lock);
}

void handle_quit_command(int client_sock) {
    char *response = "4.0 BYE Goodbye\n";
    send(client_sock, response, strlen(response), 0);
    close(client_sock);
    sem_post(&connection_semaphore);
}

bool parse_peer(const char* peer_string, char* ip, int* port) {
    char* colon = strchr(peer_string, ':');
    if (colon) {
        size_t ip_length = colon - peer_string;
        strncpy(ip, peer_string, ip_length);
        ip[ip_length] = '\0';
        *port = atoi(colon + 1);

        // Check if the IP is "localhost" and convert to "127.0.0.1"
        if (strcmp(ip, "localhost") == 0) {
            strcpy(ip, "127.0.0.1");
        } else {
            // Resolve hostname to IP address
            struct hostent *he = gethostbyname(ip);
            if (he == NULL) {
                log_message("Failed to resolve hostname: %s", ip);
                return false;
            }
            strcpy(ip, inet_ntoa(*(struct in_addr*)he->h_addr_list[0]));
        }

        log_message("Parsed peer: %s:%d", ip, *port);
        return true;
    } else {
        log_message("Invalid peer string format: %s", peer_string);
        return false;
    }
}

void read_config(const char *filename) {
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        perror("Could not open configuration file");
        exit(EXIT_FAILURE);
    }

    char line[BUFFER_SIZE];
    while (fgets(line, sizeof(line), file)) {
        char key[BUFFER_SIZE], value[BUFFER_SIZE];
        if (sscanf(line, "%[^=]=%s", key, value) == 2) {
            // Trim whitespace from key and value
            char *key_trim = strtok(key, " \t\n\r");
            char *value_trim = strtok(value, " \t\n\r");

            if (strcmp(key_trim, "BBFILE") == 0) {
                strncpy(bbfile, value_trim, BUFFER_SIZE - 1);
                bbfile[BUFFER_SIZE - 1] = '\0'; // Ensure null-termination
            } else if (strcmp(key_trim, "BBPORT") == 0) {
                bp = atoi(value_trim);
            } else if (strcmp(key_trim, "SYNCPORT") == 0) {
                sp = atoi(value_trim);
            } else if (strcmp(key_trim, "THMAX") == 0) {
                T = atoi(value_trim);
            } else if (strcmp(key_trim, "DAEMON") == 0) {
                d = atoi(value_trim);
            } else if (strcmp(key_trim, "DEBUG") == 0) {
                D = atoi(value_trim);
            } else if (strcmp(key_trim, "DELAY") == 0) {
                Y = atoi(value_trim);
            } else if (strcmp(key_trim, "PEER") == 0) {
                if (peer_count < MAX_PEERS && strlen(value_trim) > 0) {
                    char ip[INET_ADDRSTRLEN];
                    int port;
                    if (parse_peer(value_trim, ip, &port)) {
                        snprintf(peers[peer_count], BUFFER_SIZE, "%s:%d", ip, port);
                        log_message("Added peer: %s", peers[peer_count]);
                        peer_count++;
                    } else {
                        log_message("Failed to parse peer: %s", value_trim);
                    }
                }
            }
        }
        
    }
    for (int i = 0; i < peer_count; i++) {
        log_message("Parsed peer %d: %s:%d", i, manager.peers[i].ip, manager.peers[i].port);
    }
    fclose(file);
}

void log_message(const char *format, ...) {
    va_list args;
    va_start(args, format);

    time_t now;
    char time_buffer[26];
    time(&now);
    struct tm *local_time = localtime(&now);
    strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%d %H:%M:%S", local_time);

    char message[BUFFER_SIZE];
    vsnprintf(message, sizeof(message), format, args);

    if (log_file) {
        fprintf(log_file, "[%s] %s\n", time_buffer, message);
        fflush(log_file);
    }

    if (!d) {  // If not daemonized, also print to console
        printf("[%s] %s\n", time_buffer, message);
        fflush(stdout);
    }

    va_end(args);
}

void daemonize() {
    pid_t pid;
    struct rlimit rl;
    struct sigaction sa;

    // Clear file creation mask
    umask(0);

    // Get maximum number of file descriptors
    if (getrlimit(RLIMIT_NOFILE, &rl) < 0) {
        log_message("ERROR: Can't get file limit");
        exit(EXIT_FAILURE);
    }

    // Become a session leader to lose controlling TTY
    if ((pid = fork()) < 0) {
        log_message("ERROR: First fork failed: %m");
        exit(EXIT_FAILURE);
    } else if (pid > 0) {
        exit(EXIT_SUCCESS);
    }
    setsid();

    // Ensure future opens won't allocate controlling TTYs
    sa.sa_handler = SIG_IGN;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGHUP, &sa, NULL) < 0) {
        log_message("ERROR: Can't ignore SIGHUP");
        exit(EXIT_FAILURE);
    }
    if ((pid = fork()) < 0) {
        log_message("ERROR: Second fork failed: %m");
        exit(EXIT_FAILURE);
    } else if (pid > 0) {
        exit(EXIT_SUCCESS);
    }

    // Close all open file descriptors
    for (rlim_t i = 0; i < rl.rlim_max; i++) {
        if (i != (rlim_t)STDIN_FILENO && i != (rlim_t)STDOUT_FILENO && i != (rlim_t)STDERR_FILENO && i != (rlim_t)server_socket) { 
            close(i);
        }
    }
    
    // Reopen standard file descriptors to /dev/null
    int fd = open("/dev/null", O_RDWR); 
    if (fd != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > 2) {
            close(fd);
        }
    }

    // Open the log file
    char log_file_path[BUFFER_SIZE * 2];
    snprintf(log_file_path, sizeof(log_file_path), "%s/bbserv.log", original_dir);
    log_file = fopen(log_file_path, "a");
    if (log_file == NULL) {
        perror("Failed to open log file");
        exit(EXIT_FAILURE);
    }

    // Write the PID to a file
    create_pid_file();
}

void debug_log(const char *format, ...) {
    if (D) {
        va_list args;
        va_start(args, format);
        log_message(format, args);
        va_end(args);
    }
}

// Initialize the PeerNetwork
void init_peer_network(PeerNetwork* manager, int port, Peer peers[], int peer_count) {
    manager->port = port;
    manager->peer_count = peer_count;
    for (int i = 0; i < peer_count; ++i) {
        strncpy(manager->peers[i].ip, peers[i].ip, INET_ADDRSTRLEN);
        manager->peers[i].ip[INET_ADDRSTRLEN - 1] = '\0';  // Ensure null-termination
        manager->peers[i].port = peers[i].port;
        log_message("Initialized peer %d: %s:%d", i, manager->peers[i].ip, manager->peers[i].port);
    }
    pthread_mutex_init(&manager->mutex, NULL);
    manager->running = true;
    log_message("PeerNetwork initialized with %d peers", peer_count);
}

// Start the listener in a new thread
void start_listener(PeerNetwork* manager) {
    pthread_create(&manager->listener_thread, NULL, listener, manager);
}

// Main function for the listener thread
void* listener(void* arg) {
    PeerNetwork* manager = (PeerNetwork*)arg;
    int server_sock = create_socket();
    if (server_sock < 0) {
        perror("Failed to create socket");
        return NULL;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(manager->port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Failed to bind socket");
        close(server_sock);
        return NULL;
    }

    if (listen(server_sock, 10) < 0) {
        perror("Failed to listen on socket");
        close(server_sock);
        return NULL;
    }

    while (manager->running) {
        int client_sock = accept(server_sock, NULL, NULL);
        if (client_sock < 0) {
            perror("Failed to accept connection");
            continue;
        }
        handle_connection(client_sock, manager);
        close(client_sock);
    }

    close(server_sock);
    return NULL;
}

// Synchronize data with peers using the two-phase commit protocol
bool sync_peer_network(PeerNetwork* manager, const char* operation, const char* data) {
    pthread_mutex_lock(&manager->mutex);
    int peer_sockets[MAX_PEERS];
    bool ack_received[MAX_PEERS] = {false};

    log_message("Starting sync_peer_network operation: %s", operation);

    // Precommit phase
    for (int i = 0; i < manager->peer_count; ++i) {
        if (strlen(manager->peers[i].ip) == 0 || manager->peers[i].port == 0) {
            log_message("Skipping invalid peer at index %d", i);
            continue;
        }
        log_message("Attempting to connect to peer %s:%d", manager->peers[i].ip, manager->peers[i].port);
        
        peer_sockets[i] = create_socket();
        if (peer_sockets[i] < 0) {
            log_message("Failed to create socket for peer %s:%d", manager->peers[i].ip, manager->peers[i].port);
            continue;
        }
        log_message("Create Socket %s:%d", manager->peers[i].ip, manager->peers[i].port);

        struct sockaddr_in peer_addr;
        peer_addr.sin_family = AF_INET;
        peer_addr.sin_port = htons(manager->peers[i].port);
        if (inet_pton(AF_INET, manager->peers[i].ip, &peer_addr.sin_addr) <= 0) {
            log_message("Invalid peer address: %s", manager->peers[i].ip);
            close(peer_sockets[i]);
            continue;
        }
        log_message("Created Inet %s:%d", manager->peers[i].ip, manager->peers[i].port);
        if (connect(peer_sockets[i], (struct sockaddr*)&peer_addr, sizeof(peer_addr)) < 0) {
            log_message("Failed to connect to peer %s:%d - %s", 
                        manager->peers[i].ip, manager->peers[i].port, strerror(errno));
            close(peer_sockets[i]);
            continue;
        }

        log_message("Successfully connected to peer %s:%d", manager->peers[i].ip, manager->peers[i].port);

        char precommit_msg[BUFFER_SIZE];
        snprintf(precommit_msg, sizeof(precommit_msg), "PRECOMMIT %s", operation);

        if (send_message(peer_sockets[i], precommit_msg) && strcmp(receive_message(peer_sockets[i]), "ACK") == 0) {
            ack_received[i] = true;
        } else {
            log_message("Failed to get ACK from peer %s:%d", manager->peers[i].ip, manager->peers[i].port);
            close(peer_sockets[i]);
        }
    }

    // Check if all peers are ready
    bool all_ack = true;
    for (int i = 0; i < manager->peer_count; ++i) {
        if (!ack_received[i]) {
            all_ack = false;
            break;
        }
    }

    // Commit phase
    if (all_ack) {
        for (int i = 0; i < manager->peer_count; ++i) {
            char commit_msg[BUFFER_SIZE];
            snprintf(commit_msg, sizeof(commit_msg), "COMMIT %s %s", operation, data);

            if (send_message(peer_sockets[i], commit_msg) && strcmp(receive_message(peer_sockets[i]), "ACK") == 0) {
                ack_received[i] = true;
            } else {
                ack_received[i] = false;
            }
        }
    } else {
        for (int i = 0; i < manager->peer_count; ++i) {
            if (ack_received[i]) {
                send_message(peer_sockets[i], "ABORT");
            }
            close(peer_sockets[i]);
        }
        pthread_mutex_unlock(&manager->mutex);
        return false;
    }

    // Final check
    all_ack = true;
    for (int i = 0; i < manager->peer_count; ++i) {
        if (!ack_received[i]) {
            all_ack = false;
            break;
        }
    }

    if (all_ack) {
        for (int i = 0; i < manager->peer_count; ++i) {
            close(peer_sockets[i]);
        }
        pthread_mutex_unlock(&manager->mutex);
        return true;
    } else {
        for (int i = 0; i < manager->peer_count; ++i) {
            send_message(peer_sockets[i], "UNDO");
            close(peer_sockets[i]);
        }
        pthread_mutex_unlock(&manager->mutex);
        return false;
    }
}

// Handle an incoming connection (participant side)
void handle_connection(int client_sock, PeerNetwork* manager) {
    (void)manager;
    char* message = receive_message(client_sock);
    if (strncmp(message, "PRECOMMIT", 9) == 0) {
        bool ready_for_commit = true; // We'll assume we're always ready for simplicity

        if (ready_for_commit) {
            send_message(client_sock, "ACK");

            // Wait for the commit message
            char* commit_message = receive_message(client_sock);
            if (strncmp(commit_message, "COMMIT", 6) == 0) {
                char operation[10];
                char data[BUFFER_SIZE];
                sscanf(commit_message, "COMMIT %s %[^\n]", operation, data);

                bool operation_success = false;

                if (strcmp(operation, "WRITE") == 0) {
                    // For WRITE, data format is: "message username"
                    char *message, *username;
                    printf("Data: %s\n", data);
                    username = strtok(data, "/");

                // Extract message (handles spaces)
                    message = strtok(NULL, "");
                    printf("Username: %s\n", username);
                    printf("Message: %s\n", message);
                    // Create a dummy socket for handle_write_command
                    int dummy_sock = dup(client_sock);
                    handle_write_command(dummy_sock, message, username);
                    close(dummy_sock);
                    
                    operation_success = true; // Assume success for simplicity
                }
                else if (strcmp(operation, "REPLACE") == 0) {
                    // For REPLACE, data format is: "message_number message username"
                    int message_number;
                    char message[BUFFER_SIZE];
                    char username[BUFFER_SIZE];
                    sscanf(data, "%d %[^/]/%s", &message_number, message, username);
                    
                    // Create a dummy socket for handle_replace_command
                    int dummy_sock = dup(client_sock);
                    handle_replace_command(dummy_sock, message_number, message, username);
                    close(dummy_sock);
                    
                    operation_success = true; // Assume success for simplicity
                }

                if (operation_success) {
                    send_message(client_sock, "ACK");
                } else {
                    send_message(client_sock, "NACK");
                }
            }
        } else {
            send_message(client_sock, "NACK");
        }
    }
}
// Utility to create a socket
int create_socket() {
    return socket(AF_INET, SOCK_STREAM, 0);
}

// Utility to send a message over a socket
bool send_message(int sockfd, const char* message) {
    return send(sockfd, message, strlen(message), 0) != -1;
}

// Utility to receive a message from a socket
char* receive_message(int sockfd) {
    static char buffer[BUFFER_SIZE];
    memset(buffer, 0, BUFFER_SIZE);
    int bytes_received = recv(sockfd, buffer, BUFFER_SIZE - 1, 0);
    if (bytes_received < 0) {
        perror("Failed to receive message");
        return NULL;
    }
    return buffer;
}
