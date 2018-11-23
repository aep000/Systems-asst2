#include <stdio.h>
#include <string.h>


int compare(movie_data* in1,  movie_data* in2, const char* header){
	char* val1 = getStringValues(*in1,header);
	char* val2 = getStringValues(*in2,header);
	if(val1!=NULL || val2!=NULL){
		return strcmp(val1,val2);
	}
	else{
		double num1 = getNumericalValue(*in1,header);
		double num2 = getNumericalValue(*in2,header);
		return -(num1-num2);
	}
}

movie_data* mergeSort(movie_data* head, const char* searchColumn){

    if(head->next==NULL){
        return head;
    }
    int c = 0;
    int length = 0;
    movie_data* cursor = head;
    while(cursor!=NULL){
          length++;
          cursor=cursor->next;
    }
    c = 1;
    cursor = head;
    while(c<(length/2)){
        cursor = cursor->next;
	c++;
    }
    movie_data* secondHead = cursor->next;
    cursor->next=NULL;
    movie_data* l1 = mergeSort(head, searchColumn);
    movie_data* l2 = mergeSort(secondHead, searchColumn);
    movie_data* newHead = NULL;
    movie_data* tail;
    while(l1!=NULL || l2!=NULL){
        movie_data* temp;
        if(l1!=NULL && l2!=NULL){
            int cmp = compare(l1,l2,searchColumn);
            if(cmp<0){
                temp = l2;
				if(l2!=NULL){
		            		l2=l2->next;
		        	}
		            }
		            if(cmp>0){
		                temp = l1;
				if(l1!=NULL){
		            		l1=l1->next;
		        	}

		            }
		            if(cmp==0){
		                temp = l1;
				if(l1!=NULL){
		            		l1=l1->next;
		        	}
		            }
		            if(newHead==NULL){
		                newHead = temp;
		                tail = temp;
		            }
		            else{
		                tail->next = temp;
		                tail = tail->next;
		            }
		        }
		        else{
		            if(l1!=NULL){
										if(newHead==NULL){
											return l1;
										}
								    tail->next=l1;
										return newHead;
								}
								if(l2!=NULL){
									if(newHead==NULL){
										return l2;
									}
								  tail->next=l2;
										return newHead;
		            }
		        }
    }
	return newHead;


}
