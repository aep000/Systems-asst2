#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <pthread.h>
#include "multiThreadSorter_thread.h"
#include "mergesort.c"

void errorOut(char* error){
	fprintf(stderr, "%s\n",error);
	exit(-1);
}

int endsWithSlash(const char *str)
{
    if (!str)
        return 0;
    size_t lenstr = strlen(str);
    size_t lensuffix = strlen("/");
    if (lensuffix >  lenstr)
        return 0;
    return strncmp(str + lenstr - lensuffix, "/", lensuffix) == 0;
    //return (strcmp(strchr(str,'/'),"/")==0);
}
//convert to System Calls
int writeFile(char* name, movie_data* head, char* destinationPath){
	char path[1024];
	strcpy(path,destinationPath);
	if(!endsWithSlash(path))
		strcat(path,"/");
	strcat(path,name);
	FILE *fp = fopen(path, "w");
	if(!fp){
		errorOut("Can't Open File");
	}
	fprintf(fp,"%s\n",FILE_HEADER);
	movie_data* cur = head;
	int i=0;
	while(cur!=NULL){
		fprintf(fp,"%s\n",getRowString(cur));
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
int checkIfValidCSV(const char* path){
	if(strcmp(strchr(path,'.'),".csv")!=0)
		return -2;
	return 1;
}

void printDuration(movie_data * h){
	while(h!=NULL){
		//printf("%d\n",h->duration);
		h=h->next;
	}
}
movie_data * merge(movie_data * h1, movie_data * h2, const char * sortColumn){
	movie_data * out = NULL;
	if(h1==NULL){
		if(h2==NULL){
			return NULL;
		}
		return h2;
	}
	if(h2==NULL){
		return h1;
	}
	movie_data * tail;
	while(h1!=NULL && h2!=NULL){
		int comp = compare(h1,h2,sortColumn);
		movie_data * temp;
		if(comp>0){
			temp = h1;
			h1 = h1->next;
		}
		else{
			temp = h2;
			h2=h2->next;
		}
		if(out == NULL){
			out = temp;
			tail = temp;
		}
		else{
			tail->next = temp;
			tail = tail->next;
			tail->next = NULL;
		}
	}
	if(out != NULL){
		if(h1!=NULL){
			tail->next = h1;
		}
		if(h2!=NULL){
			tail->next = h2;
		}
	}
	else{
		if(h1!=NULL)
			return h1;
		if(h2!=NULL)
			return h2;
	}
	return out;

}

struct metaMergeStruct{
	tnode * head;
	int len;
	const char* sortColumn;
	movie_data * output;
	pthread_t tid;
};

void * metaMerge(void* input){
	struct metaMergeStruct * inp = (struct metaMergeStruct * ) input;
	tnode * head = inp->head;
	int len = inp->len;
	const char * sortColumn = inp->sortColumn;
	if(head == NULL){
		inp->output= NULL;
		return NULL;
	}
	//if(len == 2){
	//	return merge()
	//}
//	if(head->tid ==0){
//		head = head->next;
//	}
	if(len == 1){
		pthread_join(head->tid,NULL);
		inp->output= head->head;
		return NULL;
	}
	int c = 0;
	int pivot = len/2;
	tnode* cursor = head;
	while(c<pivot){
		cursor=cursor->next;
		c++;
	}
	//TODO thread
	char * p1 = head->dPath;
	char * p2 = cursor->dPath;
	struct metaMergeStruct * in1 =malloc(sizeof(struct metaMergeStruct));
	in1->head=cursor;
	in1->len = len-pivot;
	in1->sortColumn = sortColumn;  
	struct metaMergeStruct * in2 =malloc(sizeof(struct metaMergeStruct));
	in2->head=head;
	in2->len = pivot;
	in2->sortColumn = sortColumn;
	pthread_create(&(in1->tid), NULL, metaMerge, in1);
	pthread_create(&(in2->tid), NULL, metaMerge, in2);
	
	//movie_data * h1 = metaMerge(cursor,len-pivot,sortColumn);
	//movie_data * h2 = metaMerge(head,pivot,sortColumn);
	pthread_join(in1->tid,NULL);
	pthread_join(in2->tid,NULL);
	movie_data * out = merge(in1->head->head,in2->head->head,sortColumn);
	inp->output = out;
	return NULL;
}



void * sortFile(void * threadNode){
	tnode * result = (tnode*) threadNode;
	const char * dPath=result->dPath;
	const char * sortColumn = result->sortColumn;
	if(checkIfValidCSV(dPath)!=1){
		result->head = NULL;
		return;
	}
	movie_data* head = loadFile(dPath);
	head = mergeSort(head,sortColumn);
	result->head = head;

}

void * scan(void* input)
{
	tnode * result = (tnode*) input;
	const char * dPath=result->dPath;
	int idSrc = result->ID*10;
	pid_t pid = getpid();
	printf("Initial PID: %d\n",pid);
	printf("TIDS of all spawned threads:");
	const char * sortColumn = result->sortColumn;
	tnode * localRoot=NULL;
	tnode * localTail;
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
			if(!endsWithSlash(path))
				strcat(path,"/");
			strcat(path,cursor->d_name);
			current->dPath = path;
			current->sortColumn=sortColumn;
			current->ID = idSrc+len;
			printf("%d,",idSrc+len);
			current->next = NULL;
			if(localRoot==NULL){
				localRoot = current;
				localTail = localRoot;
			}
			else{
				localTail->next = current;
				localTail = localTail->next;
			}
			//printf("%s\n",current->dPath);
			pthread_create(&(current->tid), NULL, scan, current);
		}
		else {
			char *suffix = strrchr(cursor->d_name, '.');
			//TODO Check for valid FORMAT
			tnode * current = malloc(sizeof(tnode));
			char * path= malloc(sizeof(char)*1024);
			strcpy(path,dPath);
			if(!endsWithSlash(path))
				strcat(path,"/");
			strcat(path,cursor->d_name);
			current->dPath = path;
			current->sortColumn=sortColumn;
			current->ID = idSrc+len;
			printf("%d,",idSrc+len);
			current->next = NULL;
			if(localRoot==NULL){
				localRoot = current;
				localTail = localRoot;
			}
			else{
				localTail->next = current;
				localTail = localTail->next;
			}
			//printf("%s\n",path);
			pthread_create(&(current->tid), NULL, sortFile, current);
		}
		len+=1;
	}

	closedir(dir);
	printf("\nTotal number of threads: %d\n",len);
	struct metaMergeStruct * res = malloc(sizeof(struct metaMergeStruct));
	res->head=localRoot;
	res->len=len;
	res->sortColumn=sortColumn;
	pthread_create(&(res->tid), NULL, metaMerge, res);
	pthread_join(res->tid,NULL);
	result->head = res->output;
	//printDuration(folderRes);


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
	   oValue = "./";
  }
  //printf("Initial PID: %d\nPIDS of all child processes: ", getpid());
  //fflush(stdout);
  tnode * results = malloc(sizeof(tnode));
  results->dPath=dValue;
  results->ID=0;
  results->sortColumn=cValue;
  //scan(results);
  pthread_create(&(results->tid), NULL, scan, results);
  pthread_join(results->tid,NULL);
  char buff[100];
  snprintf(buff, sizeof(buff), "AllFiles-sorted-%s.csv", cValue);
  writeFile(buff,results->head,oValue);
  //printf("\nTotal number of processes: %d\n",end);
  return 1;

}
