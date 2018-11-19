#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <pthread.h>
#include "scannerCSVsorter.h"
#include "mergesort.c"

void errorOut(char* error){
	fprintf(stderr, "%s\n",error);
	exit(-1);
}

int endsWithSlash(const char *str)
{
    //return (strcmp(strchr(str,'/'),"/")==0);
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
int checkIfValidCSV(const char* path, const char* sortColumn){
	if(strstr(path,"-sorted-")!=NULL)
	return -1;
	if(strcmp(strchr(path,'.'),".csv")!=0)
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


movie_data * merge(movie_data * h1, movie_data * h2, const char * sortColumn){
	int len1 = strlen(h1->raw_row);
	int len2 = strlen(h2->raw_row);
	movie_data * out;
	if(len2>len1){
		out = h2;
	}
	else{
		out = h1;
	}

}

movie_data * metaMerge(tnode* head, int len, const char * sortColumn){
	//if(len == 2){
	//	return merge()
	//}
	if(len == 1){
		return(head->head);
	}
	int c = 0;
	int pivot = len/2;
	tnode* cursor = head;
	while(c<pivot){
		cursor=cursor->next;
		c++;
	}
	tnode* temp = cursor->next;
	cursor->next = NULL;
	//TODO thread
	movie_data * h1 = metaMerge(temp,len-pivot,sortColumn);
	movie_data * h2 = metaMerge(cursor,pivot,sortColumn);
	return merge(h1,h2,sortColumn);
}



void * sortFile(void * threadNode){
	tnode * result = (tnode*) threadNode;
	const char * dPath=result->dPath;
	const char * sortColumn = result->sortColumn;
	if(checkIfValidCSV(dPath,sortColumn)!=1){
		result->head = NULL;
		return;
	}
	movie_data* head = parse_csv(dPath);
	head->next = mergeSort(head->next,sortColumn);
	result->head = head;

}

void * scan(void* input)
{
	tnode * in = (tnode*) input;
	const char * dPath=in->dPath;
	const char * sortColumn = in->sortColumn;
	movie_data * result =  in->head;
	tnode * localRoot = malloc(sizeof(tnode));
	localRoot->head=NULL;
	localRoot->next = NULL;
	DIR *dir;
	struct dirent *cursor;
	int children = 0;
	if (!(dir = opendir(dPath)))
	return;
	char cwd[PATH_MAX];
	int len = 0;
	while ((cursor = readdir(dir)) != NULL) {
		//printf("%s\n",cursor->d_name);
		
		if (strcmp(cursor->d_name, ".") == 0 || strcmp(cursor->d_name, "..") == 0)
			continue;
		if (cursor->d_type == DT_DIR) {
			tnode * current = malloc(sizeof(tnode));
			char path[1024];
			//printf("DIR %s\n",cursor->d_name);
			strcpy(path,dPath);
			//if(!endsWithSlash(path))
			strcat(path,cursor->d_name);
			strcat(path,"/");
			current->dPath = path;
			current->sortColumn=sortColumn;
			current->next = localRoot->next;
			localRoot->next = current;
			//printf("%s\n",current->dPath); 
			pthread_create(&(current->tid), NULL, scan, current);
		}
		else {
			char *suffix = strrchr(cursor->d_name, '.');
			//TODO Check for valid FORMAT
			tnode * current = malloc(sizeof(tnode)); 
			char * path= malloc(sizeof(char)*1024);
			strcpy(path,dPath);
			//if(!endsWithSlash(path))
			//	strcat(path,"/");
			strcat(path,cursor->d_name);
			current->dPath = path;
			current->sortColumn=sortColumn;
			current->next = localRoot->next;
			localRoot->next = current;
			//printf("%s\n",path); 
			pthread_create(&(current->tid), NULL, sortFile, current);
		}
		len+=1;
	}
	
	closedir(dir);
	localRoot = localRoot->next;
	while(localRoot != NULL){
		pthread_join(localRoot->tid,NULL);
		printf("%s\n",localRoot->dPath);
		localRoot=localRoot->next;
	}
	return NULL;
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
  //printf("Initial PID: %d\nPIDS of all child processes: ", getpid());
  //fflush(stdout);
  tnode * results = malloc(sizeof(tnode));
  results->dPath=dValue;
  results->sortColumn=cValue;
  scan(results);
  //printf("\nTotal number of processes: %d\n",end);
  return 1;

}
