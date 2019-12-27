#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <zconf.h>
#include <arpa/inet.h>
#include <ctype.h>
#include "../include/protocol.h"

FILE *logfp;

// Initialize request array with size of 28
int request[REQUEST_MSG_SIZE];

// Function to count the first letter of the word
int count_letter(char * arrLine, int mapper_id, int sockfd) {
  FILE * fp;

  // Get rid of new line
  if (arrLine[strlen(arrLine) - 1] == '\n') {
    arrLine[strlen(arrLine) - 1] = '\0';
  }

  // open the txt file to count
  fp = fopen(arrLine, "r");
  if (fp == NULL)
  exit(EXIT_FAILURE);

  // Put all zero into the request array
  int request[REQUEST_MSG_SIZE] = {0};
  // Put all zero into the response array
  int response[RESPONSE_MSG_SIZE] = {0};
  // Assign 'UPDATE_AZLIST' as Request Command
  request[0] = UPDATE_AZLIST;
  // Assign mapper id
  request[1] = mapper_id;

  // Array to the current line in the file
  char line[1024];

  // While there are still lines in the file, continue reading
  while(fgets(line, sizeof(line), fp) != NULL) {
    // printf("line encounted\n");
    // printf("word is %s\n",line);
    // If the first character is not a space, count the character
    if (strncmp(&line[0]," ",1) != 0) {
      // Check for both upper and lower case
      if(line[0] >= 'a' && line[0] <= 'z') {
        request[(line[0]-'a')+2]++;
      } else if(line[0] >= 'A' && line[0] <= 'Z') {
        request[(line[0]-'A')+2]++;
      }
    }
  }

  // Send request to server
  write(sockfd, request, REQUEST_MSG_SIZE * sizeof(int));

  // Get response from server
  read(sockfd, response, RESPONSE_MSG_SIZE * sizeof(int));

  // Close the txt file
  fclose(fp);
  return 0;
}

// Function to get a path of txt file from Mapper.txt
void get_path_from_txt(int mapper_id, int sockfd) {
  FILE * fp;
  char * line = NULL;
  char * arrLine[1000];
  char txtPath[40];
  size_t len = 0;
  ssize_t read;

  // Assign correct name of the path
  sprintf(txtPath, "MapperInput/Mapper_%d.txt", mapper_id);

  // Open the mapper file
  fp = fopen(txtPath, "r");
  if (fp == NULL)
  exit(EXIT_FAILURE);

  int i = 1;

  // Define number of messages to send to server
  int num_of_msg = 0;

  // Read txt file line by line
  while ((read = getline(&line, &len, fp)) != -1) {
    arrLine[i] = line;
    // Call count function to count the correct txt file
    count_letter(arrLine[i], mapper_id, sockfd);
    i++;
    // Increase the number of messages every time
    num_of_msg++;
  }
  //printf("total: %d\n",i-1);

  // Print log messages
  fprintf(logfp, "[%d] UPDATE_AZLIST: %d\n", mapper_id, num_of_msg);

  fclose(fp);
  if (line){
    free(line);
  }
}

// Send this request before sending other types of requests
void checkin(int mapper_id, int sockfd) {
  // Put all zero into the request array
  int request[REQUEST_MSG_SIZE] = {0};
  // Put all zero into the response array
  int response[RESPONSE_MSG_SIZE] = {0};
  // Assign 'CHECKIN' as Request Command
  request[0] = CHECKIN;
  // Assign mapper id
  request[1] = mapper_id;
  // Send request to server
  write(sockfd, request, REQUEST_MSG_SIZE * sizeof(int));
  // Get response from server
  read(sockfd, response, RESPONSE_MSG_SIZE * sizeof(int));
  // Print log messages
  fprintf(logfp, "[%d] CHECKIN: %d %d\n", mapper_id, response[RSP_CODE], response[RSP_DATA]);
}

// Send this request to the server with PER-FILE word count results
void update_azList(int mapper_id, int sockfd) {
  get_path_from_txt(mapper_id, sockfd);
}

// If mapper clients want to send this GET_AZLIST request
// to the server, they should be already checked in
void get_azList(int mapper_id, int sockfd) {
  int request[REQUEST_MSG_SIZE] = {0};
  int response[LONG_RESPONSE_MSG_SIZE] = {0};
  char src[1000];
  char count[1000];
  request[0] = GET_AZLIST;
  request[1] = mapper_id;
  // Send request to server
  write(sockfd, request, REQUEST_MSG_SIZE * sizeof(int));
  // Get response from server
  read(sockfd, response, LONG_RESPONSE_MSG_SIZE * sizeof(int));
  sprintf(src, "[%d] GET_AZLIST: %d", mapper_id, response[RSP_CODE]);
  for (int i=2; i<28; i++) {
    sprintf(count, " %d", response[i]);
    strcat(src, count);
  }
  fprintf(logfp, "%s", src);
  fprintf(logfp, "\n");
}

// Only mapper clients can send this request and
// they should be already checked in
void get_mapper_updates(int mapper_id, int sockfd) {
  int request[REQUEST_MSG_SIZE] = {0};
  int response[RESPONSE_MSG_SIZE] = {0};
  request[0] = GET_MAPPER_UPDATES;
  request[1] = mapper_id;
  // Send request to server
  write(sockfd, request, REQUEST_MSG_SIZE * sizeof(int));
  // Get response from server
  read(sockfd, response, RESPONSE_MSG_SIZE * sizeof(int));
  fprintf(logfp, "[%d] GET_MAPPER_UPDATES: %d %d\n", mapper_id, response[RSP_CODE], response[RSP_DATA]);
}

// If mapper clients want to send this GET_AZLIST request
// to the server, they should be already checked in
void get_all_updates(int mapper_id, int sockfd) {
  int request[REQUEST_MSG_SIZE] = {0};
  int response[RESPONSE_MSG_SIZE] = {0};
  request[0] = GET_ALL_UPDATES;
  request[1] = mapper_id;
  // Send request to server
  write(sockfd, request, REQUEST_MSG_SIZE * sizeof(int));
  // Get response from server
  read(sockfd, response, RESPONSE_MSG_SIZE * sizeof(int));
  //printf("sum is %d\n", response[2]);
  fprintf(logfp, "[%d] GET_ALL_UPDATES: %d %d\n", mapper_id, response[RSP_CODE], response[RSP_DATA]);
}

// After getting a response, the mapper client closes its
// TCP connection and terminates its own process
void checkout(int mapper_id, int sockfd) {
  int request[REQUEST_MSG_SIZE] = {0};
  int response[RESPONSE_MSG_SIZE] = {0};
  request[0] = CHECKOUT;
  request[1] = mapper_id;
  // Send request to server
  write(sockfd, request, REQUEST_MSG_SIZE * sizeof(int));
  // Get response from server
  read(sockfd, response, RESPONSE_MSG_SIZE * sizeof(int));
  fprintf(logfp, "[%d] CHECKOUT: %d %d\n", mapper_id, response[RSP_CODE], response[RSP_DATA]);
  fprintf(logfp, "[%d] close connection\n", mapper_id);
  // close TCP connection and terminates its own process
  close(sockfd);
}

// Function that have all the request function
void mapperFunction(int mapper_id, int sockfd) {
  // printf("I'm in the mapper function\n");
  checkin(mapper_id, sockfd);
  update_azList(mapper_id, sockfd);
  get_azList(mapper_id, sockfd);
  get_mapper_updates(mapper_id, sockfd);
  get_all_updates(mapper_id, sockfd);
  checkout(mapper_id, sockfd);
}

// Function to create a log file
void createLogFile(void) {
  pid_t p = fork();
  if (p==0)
  execl("/bin/rm", "rm", "-rf", "log", NULL);

  wait(NULL);
  mkdir("log", ACCESSPERMS);
  logfp = fopen("log/log_client.txt", "w");
}

// Function to create new socket
int newsocket(int server_port, char *server_ip) {
  // printf("Start to define new socket\n");

  // Define new socket
  int newsock = socket(AF_INET , SOCK_STREAM , 0);

  // Specify an address to connect to
  struct sockaddr_in address;

  address.sin_family = AF_INET;
  address.sin_port = htons(server_port);
  address.sin_addr.s_addr = inet_addr(server_ip);

  // Connect it.
  if (connect(newsock, (struct sockaddr *) &address, sizeof(address)) != 0) {
    printf("New socket -- Connection Fail!\n");
  }
  fprintf(logfp, "[-1] open connection\n");

  return newsock;
}

int main(int argc, char *argv[]) {
  int mappers;
  char folderName[100] = {'\0'};
  char *server_ip;
  int server_port;

  if (argc == 5) { // 4 arguments
    strcpy(folderName, argv[1]);
    mappers = atoi(argv[2]);
    server_ip = argv[3];
    server_port = atoi(argv[4]);
    if (mappers > MAX_MAPPER_PER_MASTER) {
      printf("Maximum number of mappers is %d.\n", MAX_MAPPER_PER_MASTER);
      printf("./client <Folder Name> <# of mappers> <server IP> <server Port>\n");
      exit(1);
    }

    if (mappers < 1) {
      printf("Not enough mappers.\n");
      printf("./client <Folder Name> <# of mappers> <server IP> <server Port>\n");
      exit(1);
    }

  } else {
    printf("Invalid or less number of arguments provided\n");
    printf("./client <Folder Name> <# of mappers> <server IP> <server Port>\n");
    exit(1);
  }

  // Create log file
  createLogFile();

  // phase1 - File Path Partitioning
  traverseFS(mappers, folderName);

  // Phase2 - Mapper Clients's Deterministic Request Handling
  for (int mapper_id = 1; mapper_id <= mappers; mapper_id++) {
    pid_t pid = fork();

    // child process
    if (pid == 0) {

      // Create a TCP socket.
      int sockfd = socket(AF_INET , SOCK_STREAM , 0);

      // Specify an address to connect to
      struct sockaddr_in address;

      address.sin_family = AF_INET;
      address.sin_port = htons(server_port);
      address.sin_addr.s_addr = inet_addr(server_ip);

      // printf("before connected\n");
      // Connect it.
      if (connect(sockfd, (struct sockaddr *) &address, sizeof(address)) != 0) {
        printf("Connection Fail!\n");
      }
      fprintf(logfp, "[%d] open connection\n", mapper_id);

      // Socket connected successfully and
      // call mapper function to start work each request
      mapperFunction(mapper_id, sockfd);

      exit(0);
    }
  }

  // Every mapper should wait to run at the same time to make it sequential
  for (int mapper_id = 1; mapper_id <= mappers; mapper_id++) {
    wait(NULL);
  }

  // Phase3 - Master Client's Dynamic Request Handling (Extra Credit)

  FILE * fpt_comm;

  // Open the commands.txt
  fpt_comm = fopen("commands.txt", "r");
  if (fpt_comm == NULL) {
    printf("Open the commands.txt failed!!!\n");
    exit(EXIT_FAILURE);
  }

  char * commands = NULL;
  size_t length = 0;
  ssize_t read;
  int newsock;

  // Read commands.txt file line by line
  while ((read = getline(&commands, &length, fpt_comm)) != -1) {
    // Get rid of \n
    if (commands[strlen(commands) - 1] == '\n') {
      commands[strlen(commands) - 1] = '\0';
    }

    // printf("=========Phase3=========\n");
    // Just for testing to see if the commands is correct
    // printf("commands is %s\n", commands);

    // Convert to int from string
    int commandsINT = atoi(commands);

    switch (commandsINT) {
      case CHECKIN:
        // printf("This is CHECKIN\n");
        // Initialize new sock
        newsock = newsocket(server_port, server_ip);
        // Call the request function with mapper id = -1
        checkin(-1, newsock);
        // Print log message
        fprintf(logfp, "[-1] close connection\n");
        // Close the new sock
        close(newsock);
        break;
      case GET_AZLIST:
        // printf("This is GET_AZLIST\n");
        newsock = newsocket(server_port, server_ip);
        get_azList(-1, newsock);
        fprintf(logfp, "[-1] close connection\n");
        close(newsock);
        break;
      case GET_MAPPER_UPDATES:
        // printf("This is GET_MAPPER_UPDATES\n");
        newsock = newsocket(server_port, server_ip);
        get_mapper_updates(-1, newsock);
        fprintf(logfp, "[-1] close connection\n");
        close(newsock);
        break;
      case GET_ALL_UPDATES:
        // printf("This is GET_ALL_UPDATES\n");
        newsock = newsocket(server_port, server_ip);
        get_all_updates(-1, newsock);
        fprintf(logfp, "[-1] close connection\n");
        close(newsock);
        break;
      case CHECKOUT:
        // printf("This is CHECKOUT\n");
        newsock = newsocket(server_port, server_ip);
        checkout(-1, newsock);
        close(newsock);
        break;
      default:
        fprintf(logfp, "[-1] wrong command\n");
        // printf("Wrong command\n");
        break;
    }
  }

  // Close the commands.txt
  fclose(fpt_comm);

  // Close the log_client.txt
  fclose(logfp);
  return 0;
}
