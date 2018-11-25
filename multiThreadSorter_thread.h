#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <fcntl.h>
#include <dirent.h>
#include<pthread.h>

#define NUM_HEADERS 28



typedef struct node{
  char* data[28];
  struct node * next;
}movie_data;

char* FILE_HEADER = "color,director_name,num_critic_for_reviews,duration,director_facebook_likes,actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes";

typedef struct tnode{
	movie_data* head;
	struct tnode* next;
	char * dPath;
	const char * sortColumn;
	int ID;
	pthread_t tid;
}tnode;

typedef struct scanInput{
	char* dPath;
	char* oPath;
	char* sortColumn;
	tnode* result;
}scanInput;
char headers_array[NUM_HEADERS][256];
char movie_headers[NUM_HEADERS][256] = {

        "color",
        "director_name",
        "num_critic_for_reviews",
        "duration",
        "director_facebook_likes",
        "actor_3_facebook_likes",
        "actor_2_name",
        "actor_1_facebook_likes",
        "gross",
        "genres",
        "actor_1_name",
        "movie_title",
        "num_voted_users",
        "cast_total_facebook_likes",
        "actor_3_name",
        "facenumber_in_poster",
        "plot_keywords",
        "movie_imdb_link",
        "num_user_for_reviews",
        "language",
        "country",
        "content_rating",
        "budget",
        "title_year",
        "actor_2_facebook_likes",
        "imdb_score",
        "aspect_ratio",
        "movie_facebook_likes"

};

char* trimwhitespace (char *str);
movie_data * processFile(char* path);




char * getRowString(movie_data * node){
  if(node == NULL){
    return ",,,,,,,,,,,,,,,,,,,,,,,,,,,";
  }
  int c = 0;
  char * buffer= malloc(sizeof(char)*1024);;
  while (c<28){
    if(node->data[c]!=NULL){
      strcat(buffer, node->data[c]);
    }
    strcat(buffer, ",");
    c++;
  }
  return buffer;
}


int isNumeric(int test){
  switch (test) {
    case 2:
    return 1;
    break;
    case 3:
    return 1;
    break;
    case 4:
    return 1;
    break;
    case 5:
    return 1;
    break;
    case 7:
    return 1;
    break;
    case 8:
    return 1;
    break;
    case 12:
    return 1;
    break;
    case 13:
    return 1;
    break;
    case 15:
    return 1;
    break;
    case 18:
    return 1;
    break;
    case 22:
    return 1;
    break;
    case 23:
    return 1;
    break;
    case 24:
    return 1;
    break;
    case 25:
    return 1;
    break;
    case 26:
    return 1;
    break;
    case 27:
    return 1;
    break;
    default:
    return 0;
  }
}

unsigned long specialNum = 1178;
unsigned long hash(unsigned char *str,unsigned long hash, int size){
      int c;

      while (c = *str++)
          hash = ((hash << 5) + hash) + c;

      return hash%size;
}



int * genHashMap(){
  	int * out = malloc(sizeof(int)*50);
  	int c=0;
  	while(c<28){
  		int index = hash(movie_headers[c],specialNum,50);
  		out[index]= c;
  		c++;
  	}
  	return out;
}

  char* trimwhitespace (char *str) {

      char *str_copy = strdup(str);
      int len = strlen(str_copy);
      char *end;
      int i = 0;

      //Trim leading space
      while(str_copy[i] == ' ') {
          str_copy++;
      }

      // Trim trailing space
      int modified_length = strlen(str_copy)-1;

      while(str_copy[modified_length] == ' '||str_copy[modified_length] == '\r') {
          str_copy[modified_length] = '\0';
          modified_length--;
      }

      return str_copy;

      free(str_copy);
  }
//IF TIME MAKE THIS SPIT OUT SIZE TOO
movie_data* loadFile(const char* path){
  	movie_data * head=NULL;
  	movie_data * tail;
  	int fd = open(path,O_RDONLY);
  	char columnBuffer[1024];
  	memset(&columnBuffer[0], 0, sizeof(columnBuffer));
  	char one = 0;
  	int linecount = 0;
  	int bc=0;
  	int cc = 0;
  	int * map = genHashMap();
  	int mapping[28]={};
  	memset(&mapping[0], -1, sizeof(mapping));
  	movie_data * row = NULL;
  	int inQuotes=0;
  	int c;
  	while(read(fd, &one, 1)==1){
  		//printf("%c",one);
  		if(linecount == 0){
  			if(one != ',' && one != '\n'){
  				columnBuffer[bc++]=one;
  			}
  			else{
  					char* cleaned = trimwhitespace(columnBuffer);//Look into how I verify this works
  					int hashed = hash(cleaned,specialNum,50);
  					if(strcmp(movie_headers[map[hashed]],cleaned)!=0){
  						fprintf( stderr, "%s has invalid header: %s\n", path, cleaned);
  						return NULL; //ERROR OUT ON FILE IT IS INVALID
  					}
  					else{
  						mapping[cc] = map[hashed];
  					}
  					memset(&columnBuffer[0], 0, sizeof(columnBuffer));
  					if(one == ','){
  						cc++;
  					}
  					else{
  						linecount++;
  						cc=0;
  					}
  					bc=0;
  				}
  			}
  			else{ //Linecout > 0
  				if(row == NULL){
  					row = malloc(sizeof(movie_data));
  				}
  				if((one != ',' && one != '\n') || inQuotes){
  					if(one == '"'){
  						inQuotes = inQuotes ^ 1;
  					}
  					columnBuffer[bc++]=one;
  				}
  				else{
  					if(mapping[cc] ==-1){
  						fprintf( stderr, "%s has too many columns in the data", path);
  						return NULL;
  					}
  					row->data[mapping[cc]]=malloc(sizeof(char) * (strlen(columnBuffer) + 1));
  					strcpy(row->data[mapping[cc]],trimwhitespace(columnBuffer));
  					if(one == ','){
  						cc++;
  					}
  					else{
  						if(head==NULL){
  							head = row;
  							tail = head;
  						}
  						else{
  							tail->next = row;
  							tail = tail->next;
  						}
  						linecount++;
  						cc=0;
  						row = NULL;
  					}
  					bc=0;
  					memset(&columnBuffer[0], 0, sizeof(columnBuffer));
  				}
  			}
  		}
  		return head;
  	}
