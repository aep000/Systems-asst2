#include <stdio.h>
#include <stdlib.h>
#include <string.h>
unsigned long
hash(unsigned char *str,unsigned long hash)
{
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}

char movie_headers[28][256] = {

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

int main()
{
    int bad =0;
    unsigned long h=1;
    while(!bad){
        h++;
        int c = 0;
        int * used= malloc(sizeof(int)*50);
        while(c<28){
            printf("%d\n", hash(movie_headers[c],h)%50);
            if(used[hash(movie_headers[c],h)%50]==1){
                printf("BAD\n\n");
                break;
            }
            else{
                used[hash(movie_headers[c],h)%50]=1;
            }
            c++;
        }
        if(c==28){
            bad = 1;
            break;
        }
        
    }
    printf("GOT ONE: %d",h);
    

    return 0;
}
