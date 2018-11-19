#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include<pthread.h> 

typedef struct node{
	struct node* next;
	pthread_t tid;
	int value;
}node;

pthread_t tid;
struct node* head;
pthread_mutex_t lock;

void * createNode(void* result){
	int c=0;
	
	node *n = (node*) result;
	//printf("%d\n",n->next);
	while(c<(n->value*n->value)){
		c++;
	}
	n->value=c;
	return NULL;
}

int main(void) {
	head = malloc(sizeof(struct node));
	head->next=NULL;
	head->value=-1;
	int c = 0;
	node* last = head;
	while(c<10){
		node * arg = malloc(sizeof(struct node));
		arg->value = 10*c;
		arg->next = head->next;
		head->next = arg;
		pthread_create(&(arg->tid), NULL, &createNode, arg);
		c+=1;
	}
	printf("Moving to print\n");
	head = head->next;
	while(head !=NULL){
		pthread_join(head->tid,NULL);
		printf("%d\n",head->value);
		head= head->next;
	}
	printf("AFTER THREAD\n");
	
}
