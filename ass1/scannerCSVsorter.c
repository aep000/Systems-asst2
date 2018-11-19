#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include "scannerCSVsorter.h"
#include "mergesort.c"


void errorOut(char* error){
	fprintf(stderr, "%s\n",error); exit(-1);
}

int endsWithSlash(const char *str)
{
    return (strcmp(strchr(str,'/'),"/")==0);
    return (str && *str && str[strlen(str) - 1] == '/') ? 0 : 1;
}

int writeFile(char* name, movie_data* head,char* sortingHead, char* destinationPath){
	char path[1024];
	strcpy(path,destinationPath);
	if(!endsWithSlash(path))
		strcat(path,"/");
	strcat(path,name);
	int i = 0;
    	while(path[i] != '\0')
    	{
        	i++;

    	}
    	path[i-4] = '\0';
	strcat(path,"-sorted-");
	strcat(path,sortingHead);
	strcat(path,".csv");
	FILE *fp = fopen(path, "w");
	if(!fp){
		errorOut("Can't Open File");
	}
	movie_data* cur = head;
	i=0;
	while(cur!=NULL){
		fprintf(fp,"%s\n",cur->raw_row);
		cur = cur->next;
		i++;
	}
	fclose(fp);
	return i;
}

int checkValidDir(char* path){

    DIR * dir = opendir(path);
    struct dirent *ent;
    if(!dir) {
        return 0;
    }
    return 1;
}

int checkValidParamConfig( int cFlag, char* cValue, int oFlag, char* oValue, int dFlag, char* dValue){
    if(!cFlag)
	return -1;
    if(oFlag && !checkValidDir(oValue))
	return -2;
    if(dFlag && !checkValidDir(dValue))
	return -3;
    char* trimmed = trimwhitespace(cValue);
    int i = 0;
    while(i<NUM_HEADERS){
	if(strcmp(trimmed,movie_headers[i])==0){
		i=99;
	}
	i++;
    }
    if(i!=100){
    	return -4;
    }
    return 1;

}
int checkIfValidCSV(char* path, const char* sortColumn){
    if(strstr(path,"-sorted-")!=NULL)
	return -1;
    if(strcmp(strstr(path,".csv"),".csv")!=0)
	return -2;
    FILE *fp = fopen(path,"r");
    char buffer[1024];
    if(fp){
	while (fgets(buffer, sizeof(buffer), fp) != NULL)
    	{
        	buffer[strlen(buffer) - 1] = '\0'; // eat the newline fgets() stores
		strcat(buffer,",");
		char* temp = strdup(buffer);
		strcpy(temp,buffer);
		char* last = temp;
		int sortColumnInside=0;
		while((temp = strstr(temp,","))!=NULL){

			*temp='\0';
			temp = temp+1;
			char* trimmed = trimwhitespace(last);
			int i = 0;
			while(i<NUM_HEADERS){
			    if(strcmp(trimmed,movie_headers[i])==0){
				i=99;
			    }
			    i++;
			}
			if(i!=100){
				return -4;
			}
			if(strcmp(trimmed,sortColumn)==0){
				sortColumnInside=1;
			}
			last=temp;

		}
		if(sortColumnInside!=1){
			return -5;
		}
		break;
         	//printf("%s/n", buffer);
    	}
    }
    else{
	return -3;
    }
    return 1;
}
int scan(const char *dPath, const char *oPath, const char *sortColumn)
{
  DIR *dir;
  struct dirent *cursor;
  int children = 0;
  if (!(dir = opendir(dPath)))
    return;
  char cwd[PATH_MAX];
  while ((cursor = readdir(dir)) != NULL) {
    pid_t child;
    //printf("%s\n",cursor->d_name);
    if (cursor->d_type == DT_DIR) {
      char path[1024];
      if (strcmp(cursor->d_name, ".") == 0 || strcmp(cursor->d_name, "..") == 0)
        	continue;
      //printf("DIR %s\n",cursor->d_name);
      child = fork();
      if(child == 0){
        strcpy(path,dPath);
	if(!endsWithSlash(path))
        	strcat(path,"/");
        strcat(path,cursor->d_name);
        //printf("%d,", getpid());
        fflush(stdout);
        int subchildren = scan(path,oPath,sortColumn);
        //free(path);
        exit(subchildren);
        break;
      }
      else{
        int status;
        if ( waitpid(child, &status, 0) == -1 ) {
          perror("WAIT FAILED");
          return 0;
        }
        children += WEXITSTATUS(status)+1;
        //printf("%d\n",child);
        //printf("%s %d\n",cursor->d_name, child);
        continue;
      }
    }
    else {
      char *suffix = strrchr(cursor->d_name, '.');
      child=fork();
      if(child==0){
        //TODO Check for valid FORMAT
        char path[1024];
        strcpy(path,dPath);
	if(!endsWithSlash(path))
        	strcat(path,"/");
        strcat(path,cursor->d_name);
	printf("%d,",  getpid());
        fflush(stdout);
	if(checkIfValidCSV(path,sortColumn)!=1){
		exit(-1);
	}
        movie_data* head = parse_csv(path);
	head->next = mergeSort(head->next,sortColumn);
	if(oPath){
		strcpy(path,oPath);
	}
	else{
		strcpy(path,dPath);
	}
	writeFile(cursor->d_name,head,sortColumn,path);
        exit(1);
      }
      else{
        children++;
        //printf("%s %d\n",cursor->d_name, getpid() );
        continue;
      }
    }
  }
  closedir(dir);
  return children;
}

int main(int argc, char *argv[]) {
  if(argc<3){
	errorOut("Too few arguments exiting");
  }
  if(argc%2==0){
	errorOut("Missing value exiting");
  }
  int cFlag = 0;
  char* cValue;
  int oFlag = 0;
  char* oValue;
  int dFlag = 0;
  char* dValue;
  int i = 1;
  while(i<argc){
  	if(strcmp(argv[i],"-c")==0){
  		cFlag=1;
  		cValue=argv[i+1];
  	}
  	else if(strcmp(argv[i],"-o")==0){
  		oFlag=1;
  		oValue = argv[i+1];
  	}
  	else if(strcmp(argv[i],"-d")==0){
  		dFlag=1;
  		dValue = argv[i+1];

  	}
  	else{
  		errorOut("Unkown flag exiting");
  		//fprintf(stderr, "Unkown Flag\n");
  	}
  	i+=2;
  }
  int paramCode = checkValidParamConfig(cFlag, cValue, oFlag, oValue, dFlag, dValue);
  if(paramCode == -2 || paramCode == -3){
	   errorOut("Invalid Directory as arg exiting");
  }
  if(paramCode == -1){
	   errorOut("No -c flag exiting");
  }
  if(paramCode==-4){
	   errorOut("Invalid column heading exiting");
  }
  if(dFlag == 0){
	   dValue = "./";
  }
  if(oFlag == 0){
	   oValue = NULL;
  }
  printf("Initial PID: %d\nPIDS of all child processes: ", getpid());
  fflush(stdout);
  int end = scan(dValue,oValue,cValue);
  pid_t wpid;
  int status = 0;
  while ((wpid = wait(&status)) > 0)
  {
  }
  printf("\nTotal number of processes: %d\n",end);
  return 1;

}
