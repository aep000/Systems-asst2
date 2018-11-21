#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include<pthread.h>

#define NUM_HEADERS 28
typedef struct node {
            char *raw_row;
            char *color;
            char *director_name;
            int num_critic_for_reviews;
            int duration;
            int director_facebook_likes;
            int actor_3_facebook_likes;
            char *actor_2_name;
            int actor_1_facebook_likes;
            double gross;
            char *genres;
            char *actor_1_name;
            char *movie_title;
            int num_voted_users;
            int cast_total_facebook_likes;
            char *actor_3_name;
            int facenumber_in_poster;
            char *plot_keywords;
            char *movie_imdb_link;
            int num_user_for_reviews;
            char *language;
            char *country;
            char *content_rating;
            double budget;
            int title_year;
            int actor_2_facebook_likes;
            double imdb_score;
            double aspect_ratio;
            int movie_facebook_likes;
            struct node *next;
}movie_data;

char* FILE_HEADER = "color,director_name,num_critic_for_reviews,duration,director_facebook_likes,actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes";

typedef struct tnode{
	movie_data* head;
	struct tnode* next;
	char * dPath;
	char * sortColumn;
	pthread_t tid;
}tnode;

typedef struct scanInput{
	char* dPath;
	char* oPath;
	char* sortColumn;
	tnode* result;
}scanInput;

char *sort_by;
char search_dir_path[5000];
char output_dir_path[5000];
char output_file_path[5000];
char *filename;
char sorted_filename[256];
DIR *search_dir;
DIR *output_dir;
char cwd[256];
char headers_array[NUM_HEADERS][256];

movie_data *front;
//int pos_sort;
char headers_array[NUM_HEADERS][256];
//int header_comma_count = 0;
int comma_match;
int valid_csv = 1;
char *headers_string;
#define NUM_HEADERS 28

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


void check_parameters(int argc, char *argv[]);


movie_data* get_last_node();
char* trimwhitespace (char *str);
void push (movie_data *new_node);
void store_headers(char *headers, int header_comms_count);
void build_movie_data_node(char *data_str, int header_comma_count);
void assign_output(movie_data *output, int index, char *value);
movie_data* parse_csv(char *filename);
/*
 *  * ALWAYS CALL THIS FIRST AND CHECK FOR NULL getNumericalValue WILL FAIL IF SEARCHING FOR NON NUMERICAL VALUE
 *   */
char* getStringValues(movie_data head, char* header){
        if(!strcmp(header,"color")){
            return head.color;
        }
        if(!strcmp(header,"director_name")){
            return head.director_name;
        }
        if(!strcmp(header,"actor_2_name")){
            return head.actor_2_name ;
        }
        if(!strcmp(header,"actor_1_name")){
            return head.actor_1_name ;
        }
        if(!strcmp(header,"actor_3_name")){
            return head.actor_3_name ;
        }
        if(!strcmp(header,"genres")){
            return head.genres ;
        }
        if(!strcmp(header,"movie_title")){
            return head.movie_title ;
        }
        if(!strcmp(header,"plot_keywords")){
            return head.plot_keywords ;
        }
        if(!strcmp(header,"movie_imdb_link")){
            return head.movie_imdb_link ;
        }
        if(!strcmp(header,"language")){
            return head.language ;
        }
        if(!strcmp(header,"country")){
            return head.country ;
        }
        if(!strcmp(header,"content_rating")){
            return head.content_rating ;
        }
	return NULL;


};
double getNumericalValue(movie_data head, char* header){
	if(!strcmp(header,"num_critic_for_reviews")){
	    return head.num_critic_for_reviews;
	}
	if(!strcmp(header,"duration")){
        return head.duration;
	}
	if(!strcmp(header,"director_facebook_likes")){
        return head.director_facebook_likes;
	}
	if(!strcmp(header,"actor_3_facebook_likes")){
        return head.actor_3_facebook_likes;
	}
	if(!strcmp(header,"actor_1_facebook_likes")){
		return head.actor_1_facebook_likes;
	}
	if(!strcmp(header,"num_voted_users")){
	    return head.num_voted_users;

	}
	if(!strcmp(header,"cast_total_facebook_likes")){
		return head.cast_total_facebook_likes;
	}
	if(!strcmp(header,"facenumber_in_poster")){
        return head.facenumber_in_poster;
	}
	if(!strcmp(header,"num_user_for_reviews")){
        return head.num_user_for_reviews;
	}
	if(!strcmp(header,"title_year")){
        return head.title_year;

	}
	if(!strcmp(header,"actor_2_facebook_likes")){
        return head.actor_2_facebook_likes;
	}
	if(!strcmp(header,"movie_facebook_likes")){
        return head.movie_facebook_likes;
	}
    if(!strcmp(header,"aspect_ratio")){
        return head.aspect_ratio;
    }
    if(!strcmp(header,"imdb_score")){
        return head.imdb_score;
    }
    if(!strcmp(header,"budget")){
        return head.budget;
    }
    if(!strcmp(header,"gross")){
        return head.gross;
    }
}
char * getRow(movie_data * node){
	char buff[1024];
	sprintf(buff,"%s,%s,%d,%d,%d,%d,%s,%d,%d,%s,%s,%s,%d,%d,%s,%d,%s,%s,%d,%s,%s,%s,%d,%d,%d,%d,%d,%d",node->color,node->director_name,node->num_critic_for_reviews,node->duration,node->director_facebook_likes,node->actor_3_facebook_likes,node->actor_2_name,node->actor_1_facebook_likes,node->gross,node->genres,node->actor_1_name,node->movie_title,node->num_voted_users,node->cast_total_facebook_likes,node->actor_3_name,node->facenumber_in_poster,node->plot_keywords,node->movie_imdb_link,node->num_user_for_reviews,node->language,node->country,node->content_rating,node->budget,node->title_year,node->actor_2_facebook_likes,node->imdb_score,node->aspect_ratio,node->movie_facebook_likes);
	return buff;

}
//GOOD
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

void push (movie_data *new_node) {
    movie_data *old_node = get_last_node();
    old_node->next = new_node;
    new_node->next = NULL;
}

// returns last node in linked list

movie_data* get_last_node() {
    movie_data *current = front;
    while (current->next != NULL) {
        current = current->next;
    }
    return current;
}
//GOOD
void store_headers(char *headers, int header_comma_count) {

    int str_length = strlen(headers);
    int count = 0;
    int index = 0;
    int i = 0;

    char header[str_length];

    for(i = 0; i < str_length; i++) {

        if(headers[i] == ',') {
            header[index] = '\0';
            strcpy(headers_array[count++], trimwhitespace(header));
            strcpy(header,"");
            index = 0;

        } else if (headers[i] == '\r') { //ignore carriage returns used in Windows line endings, see http://www.cs.toronto.edu/~krueger/csc209h/tut/line-endings.html
            continue;

        } else {
            header[index++] = headers[i];
        }
    }

    header[index] = '\0';
    strcpy(headers_array[count++], trimwhitespace(header));

}

//GOOD
void build_movie_data_node(char *data_str, int header_comma_count) {

    int str_length = strlen(data_str);
    char row_element[str_length];
    int i = 0;
    int count = 0;
    int char_count = 0;
    int double_quote_mode = 1;

    movie_data *output;
    output = malloc(sizeof(movie_data));

    for(i = 0; i < strlen(data_str); i++) {

        if(data_str[i] == '\"') {
            if(double_quote_mode == 1) {
                double_quote_mode = 0;
            } else {
                double_quote_mode = 1;
            }
        } else if(data_str[i] == ',' && double_quote_mode == 1) {
            row_element[char_count] = '\0';
            assign_output(output, count++, row_element);
            strcpy(row_element, "");
            char_count = 0;
        } else {
            row_element[char_count++] = data_str[i];
        }
    }

    row_element[char_count] = '\0';
    assign_output(output, count, row_element);

    output->raw_row = strdup(data_str);

    push(output);
}

//GOOD
void assign_output(movie_data *output, int index, char *value) {
    if(strcmp(headers_array[index], "color") == 0) {
        output->color = strdup(value);
    } else if(strcmp(headers_array[index], "director_name") == 0) {
        output->director_name = strdup(value);
    } else if(strcmp(headers_array[index], "num_critic_for_reviews") == 0) {
        output->num_critic_for_reviews = atoi(value);
    } else if(strcmp(headers_array[index], "duration") == 0) {
        output->duration = atoi(value);
    } else if(strcmp(headers_array[index], "director_facebook_likes") == 0) {
        output->director_facebook_likes = atoi(value);
    } else if(strcmp(headers_array[index], "actor_3_facebook_likes") == 0) {
        output->actor_3_facebook_likes = atoi(value);
    } else if(strcmp(headers_array[index], "actor_2_name") == 0) {
        output->actor_2_name = strdup(value);
    } else if(strcmp(headers_array[index], "actor_1_facebook_likes") == 0) {
        output->actor_1_facebook_likes = atoi(value);
    } else if(strcmp(headers_array[index], "gross") == 0) {
        output->gross = atof(value);
    } else if(strcmp(headers_array[index], "genres") == 0) {
        output->genres = strdup(value);
    } else if(strcmp(headers_array[index], "actor_1_name") == 0) {
        output->actor_1_name = strdup(value);
    } else if(strcmp(headers_array[index], "movie_title") == 0) {
        output->movie_title = strdup(value);
    } else if(strcmp(headers_array[index], "num_voted_users") == 0) {
        output->num_voted_users = atoi(value);
    } else if(strcmp(headers_array[index], "cast_total_facebook_likes") == 0) {
        output->cast_total_facebook_likes = atoi(value);
    } else if(strcmp(headers_array[index], "actor_3_name") == 0) {
        output->actor_3_name = strdup(value);
    } else if(strcmp(headers_array[index], "facenumber_in_poster") == 0) {
        output->facenumber_in_poster = atoi(value);
    } else if(strcmp(headers_array[index], "plot_keywords") == 0) {
        output->plot_keywords = strdup(value);
    } else if(strcmp(headers_array[index], "movie_imdb_link") == 0) {
        output->movie_imdb_link = strdup(value);
    } else if(strcmp(headers_array[index], "num_user_for_reviews") == 0) {
        output->num_user_for_reviews = atoi(value);
    } else if(strcmp(headers_array[index], "language") == 0) {
        output->language = strdup(value);
    } else if(strcmp(headers_array[index], "country") == 0) {
        output->country = strdup(value);
    } else if(strcmp(headers_array[index], "content_rating") == 0) {
        output->content_rating = strdup(value);
    } else if(strcmp(headers_array[index], "budget") == 0) {
        output->budget = atof(value);
    } else if(strcmp(headers_array[index], "title_year") == 0) {
        output->title_year = atoi(value);
    } else if(strcmp(headers_array[index], "actor_2_facebook_likes") == 0) {
        output->actor_2_facebook_likes = atoi(value);
    } else if(strcmp(headers_array[index], "imdb_score") == 0) {
        output->imdb_score = atof(value);
    } else if(strcmp(headers_array[index], "aspect_ratio") == 0) {
        output->aspect_ratio = atof(value);
    } else if(strcmp(headers_array[index], "movie_facebook_likes") == 0) {
        output->movie_facebook_likes = atoi(value);
    }
}

char* get_headers_str() {

    return headers_string;
}

//GOOD
movie_data* parse_csv(char *filename) {


char *mode = "r";

FILE *f;

f = fopen(filename, mode);


    front = NULL;
    front = malloc(sizeof(movie_data)); //initialization and memory allocation for front node
    front->next = NULL;

    int buffer_size = 0;
    int char_count = 0;

    char *tmp;
    char *row;
    movie_data *output;
    int header_comma_count = 0;

    row = malloc(sizeof(char) * (buffer_size+1));
    row[buffer_size++] = '\0';

    int row_count = 0;

    char current_char;

    while((current_char = getc(f)) != EOF) {

        char_count++;

        if(current_char == '\n') {

        char *tmp_str = malloc(sizeof(char) *(char_count + 1));

            strcpy(tmp_str, row);

            if(row_count == 0) {
                store_headers(tmp_str, header_comma_count); //FIRST OF 2EXTERNALS

                output = malloc(sizeof(movie_data));
                output->raw_row = strdup(tmp_str);
                push(output);

            } else {
                build_movie_data_node(tmp_str, header_comma_count); //SECOND OF 2Externals
            }

            //printf("\"%s\"\n", trimwhitespace(tmp));

            free(tmp_str);

            buffer_size = 0;
            free(row);
            row = malloc(sizeof(char) * (buffer_size + 1));
            row[buffer_size++] = '\0';

            char_count = 0;
            row_count++;

        } else {
            tmp = realloc(row, buffer_size + 1);

            if(tmp != NULL) {
                row = tmp;
                row[buffer_size - 1] = current_char;
                row[buffer_size++] = '\0';
            }
        }
    }


    free(row);

    return (front->next);

}
