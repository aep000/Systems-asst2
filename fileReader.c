#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

typedef struct movieNode{
	char** rows;
	struct movieNode * next;
}movieNode;

movieNode * processFile(char* path){
	int ** headerPositions=malloc(sizeof(int*26))
}

int main(int argc, char *argv[]){
	
}
