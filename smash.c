#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include<sys/wait.h> 
#include<fcntl.h>
#include <ctype.h>


char error_message[30] = "An error has occurred\n";
char **paths;
int path_length;
int red = 0;
FILE *output = NULL;
char *built_in[3] = {"exit", "cd", "path"};

void print_exit(){
    write(STDERR_FILENO, error_message, strlen(error_message)); 
    exit(1);
}

char* find_command(char **command, char **paths){
    char *find_path = NULL;
    for(int i = 0; i < path_length; i ++){
        char *search_path = malloc(sizeof(char));
        strcat(search_path, paths[i]);
        strcat(search_path, "/");
        strcat(search_path, *command);
        int res = access(search_path, X_OK);
        if(res == 0){
            find_path = search_path;
            break;
        }
    }
    return find_path;
}

char** parse_command(char *command){
    
    
    char *pieces = strdup(command);
    char *piece = strsep(&pieces, " ");
    char *count_temp = strdup(command);
    char *count_piece = strsep(&count_temp, " ");
    int i = 0;
    int count = 0;
    while(count_piece != NULL){
        count ++;
        count_piece = strsep(&count_temp, " ");
    }
    char **res = malloc(sizeof(char*) * count);
    while(piece != NULL){
        if(strcmp(piece, "") == 0){
            piece = strsep(&pieces, " ");
            continue;
        }
        res[i] = strdup(piece);
        i ++;
        piece = strsep(&pieces, " ");
    }
    *(res + i) = NULL;
    return res;
}

void execute_command(char *path, char** commands, char* file) { 
    pid_t pid = fork();  
    if (pid == -1) { 
        print_exit();
    } 
    else if (pid == 0) { 
        if(red == 1){
            output =fopen(file, "w");
            int file_num = fileno(output);
            dup2(file_num, 1); 
            dup2(file_num, 2);
        }
        if (execv(path, commands) == -1) { 
            print_exit(); 
        } 
        exit(0); 
    } 
    else { 
        pid_t wait_id;
        do{
            wait_id = wait(NULL);
        } while(wait_id == 0);
        return; 
    } 
}

int smash_cd(char *command){
    char **res = malloc(sizeof(char*));
    char *pieces = strdup(command);
    char *piece = strsep(&pieces, " ");
    int i = 0;
    while(piece != NULL){
        *(res + i) = malloc(strlen(piece) * sizeof(char));
        *(res + i) = piece; 
        i ++;
        piece = strsep(&pieces, " ");
    }
    if(i != 2){
        return -1;
    }
    else{
        if(chdir(res[1]) != 0){
            return -1;
        }
        return 0;
    }
}

int smash_path(char *command){
    
    char *options[3] = {"add", "remove", "clear"};
    char **res = malloc(sizeof(char*));
    char *pieces = strdup(command);
    char *piece = strsep(&pieces, " ");
    int i = 0;
    while(piece != NULL){
        *(res + i) = malloc(strlen(piece) * sizeof(char));
        *(res + i) = piece; 
        i ++;
        piece = strsep(&pieces, " ");
    }
    if(i < 2 || i > 3){
        return -1;
    }
    else{
        if(strcmp(res[1], options[0]) == 0){
            if(i == 2){
                return -1;
            }
            char *path = res[2];
            if(path_length == 0){
                paths = malloc(sizeof(char*));
                paths[0] = strdup(path);
                path_length++;
            }
            else{
                path_length++;
                paths = realloc(paths, path_length *  sizeof(char*)); 
                for(int i = 0; i < path_length; i ++){
                    paths[i + 1] =  strdup(paths[i]);
                } 
                paths[0] = strdup(res[2]);
                
            }
        }
        else if(strcmp(res[1], options[1]) == 0){
            if(i == 2){
                return -1;
            }
            char *path = res[2];
            for(int i = 0; i < path_length; i ++){
                if(strcmp(path, paths[i]) == 0){
                    paths[i] = NULL;
                    path_length--;
                    return 0;
                }
            }
            return -1;
        }
        else if(strcmp(res[1], options[2]) == 0){
            if(i == 3){
                return -1;
            }
            paths = NULL;
            path_length = 0;
        }
        else{
            return -1;
        }
        return 0;
    }
}

int redirection(char* command){
    char *ptr;
    if( (ptr = strchr(command, '>')) == NULL){
        return 0;
    }
    char *pieces = strdup(command);
    char *piece = strsep(&pieces, ">");
    int i = 0;
    while(piece != NULL){
        i ++;
        piece = strsep(&pieces, ">");
    }
    if(i > 2){
        return -1;
    }
    else if(i == 2){
        return 1;
    }
    else{
        return 0;
    }
}


char *trim(char *s) {
    char *ptr;
    if (!s)
        return NULL; 
    if (!*s)
        return NULL;  
    for (ptr = s + strlen(s) - 1; (ptr >= s) && isspace(*ptr); --ptr);
    ptr[1] = '\0';

    while( isspace(*s) )
  {
    s++;
  }
    return s;
}

void execute_single(char* command, char* file_name){
    char* current_command = strdup(command);
                char* first = strsep(&current_command, " ");
                if(strcmp(first, built_in[0]) == 0){
                    char* test = strdup(command);
                    char* next = strsep(&test, " ");
                    next = strsep(&test, " ");
                    if(next != NULL){
                        write(STDERR_FILENO, error_message, strlen(error_message)); 
                    }
                    else{
                        exit(0);
                    }
                    
                }
                else if(strcmp(first, built_in[1]) == 0){
                    int status = smash_cd(command);
                    if(status != 0){
                        write(STDERR_FILENO, error_message, strlen(error_message)); 
                    }
                }
                else if(strcmp(first, built_in[2]) == 0){
                    int status = smash_path(command);
                    if(status != 0){
                        write(STDERR_FILENO, error_message, strlen(error_message)); 
                    }
                }
                else{
                    char **parsed_command = parse_command(command);
                    char *exe_path = find_command(parsed_command, paths);
                    if(exe_path == NULL){
                        write(STDERR_FILENO, error_message, strlen(error_message)); 
                    }
                    else{
                        execute_command(exe_path, parsed_command,file_name);
                    }
                }
    return;
}

int main(int argc, char *argv[]){
    path_length = 1;
    char *file_name = "";

    paths = malloc(sizeof(char*) * path_length);
    paths[0] = strdup("/bin");
    
    if(argc == 1){
        while(1){
            printf("smash> ");
            fflush(stdout);
            char *line = NULL;
            size_t len = 0;
            getline(&line, &len, stdin);
            char *ptr;
            if( (ptr = strchr(line, '\n')) != NULL){
                *ptr = '\0';
            }
            char* commands = strdup(line);
            char* command = strsep(&commands, ";");
            while(command != NULL){
                
                char *paralell = strdup(command);
                char *single = strsep(&paralell, "&");
                while(single != NULL){
                    red = redirection(single);
                if(red == -1){
                    write(STDERR_FILENO, error_message, strlen(error_message)); 
                    single = strsep(&paralell, "&"); 
                    continue;
                }
                if(red == 1){
                    char* buffer = strdup(single);
                    char* read_command = strsep(&buffer, ">");
                    char* temp_name = strsep(&buffer, ">");
                    file_name = trim(temp_name);
                    char* trimed_real_command = trim(read_command);
                    if(file_name== NULL){
                        write(STDERR_FILENO, error_message, strlen(error_message)); 
                        single = strsep(&paralell, "&"); 
                        continue;
                    }
                    single = trimed_real_command;
                }
                    execute_single(single, file_name);
                    single = strsep(&paralell, "&");
                }
                
                command = strsep(&commands, ";"); 
            }
        }
    }
    else if(argc == 2){
        FILE *fp = fopen(argv[1], "r");
        if(fp == NULL){
            print_exit();
        }
        char *line = NULL;
        size_t len = 0;
        ssize_t read;
        while((read = getline(&line, &len, fp)) != -1){
            char *ptr;
            if( (ptr = strchr(line, '\n')) != NULL){
                *ptr = '\0';
            }
            char *ptr2;
            if( (ptr2 = strchr(line, '\r')) != NULL){
                *ptr2 = '\0';
            }
            char* commands = strdup(line);
            char* command = strsep(&commands, ";");
            while(command != NULL){
                
                char *paralell = strdup(command);
                char *single = strsep(&paralell, "&");
                while(single != NULL){
                     red = redirection(single);
                if(red == -1){
                    write(STDERR_FILENO, error_message, strlen(error_message)); 
                    single = strsep(&paralell, "&"); 
                    continue;
                }
                if(red == 1){
                    char* buffer = strdup(single);
                    char* read_command = strsep(&buffer, ">");
                    char* temp_name = strsep(&buffer, ">");
                    file_name = trim(temp_name);
                    char* trimed_real_command = trim(read_command);
                    if(file_name== NULL){
                        write(STDERR_FILENO, error_message, strlen(error_message)); 
                        single = strsep(&paralell, "&"); 
                        continue;
                        
                    }
                    single = trimed_real_command;
                }
                    execute_single(single, file_name);
                    single = strsep(&paralell, "&");
                }
                command = strsep(&commands, ";"); 
            }
        }
            
    }
    else{
        print_exit();
    }
}