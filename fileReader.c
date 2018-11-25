#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

typedef struct movieNode{
	char* data[28];
	struct movieNode * next;
}movieNode;

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

unsigned long specialNum = 1178;
unsigned long hash(unsigned char *str,unsigned long hash, int size)
{
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash%size;
}



int * genHashMap(){
	int * out = malloc(sizeof(int)*50);
	c=0;
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

movieNode * processFile(char* path){
	movieNode * head;
	movieNode * tail;
	int ** headerPositions=malloc(sizeof(int*26));
	int fd = open(path);
	char columnBuffer[1024];
	char one = 0;
	int linecount = 0;
	int bc=0;
	int cc = 0;
	int * map = genHashMap();
	int mapping[28];
	while(c = read(fd, &one, 1)){
		if(linecount == 0){
			if(one != ',' || one != '\n'){
				columnBuffer[bc++]=one;
			}
			else{
					char* cleaned = trimwhitespace(columnBuffer);//Look into how I verify this works
					int hashed = hash(cleaned,specialNum,50);
					free(cleaned);
					if(strcmp(movie_headers[map[hashed]])!=0){
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
				}
			}
			else{ //Linecout > 0
				movieNode * row = malloc(sizeof(movieNode));
				if(one != ',' || one != '\n'){
					columnBuffer[bc++]=one;
				}
				else{
					if(columnBuffer!=)
					row->data[cc]=malloc(sizeof(char) * (strlen(columnBuffer) + 1));
					stcpy(row->data[cc],&columnBuffer);
					if(one == ','){
						cc++;
					}
					else{
						if(out==NULL){
							out = row;
							tail = out;
						}
						else{
							tail->next = out;
							tail = tail->next;
						}
						linecount++;
						cc=0;
					}
				}
			}
		}
	}
}

int main(int argc, char *argv[]){
	out = processFile(*argv[0]);
	return 1;
}
