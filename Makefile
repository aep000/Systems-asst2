all: multiThreadSorter_thread.c
	gcc multiThreadSorter_thread.c -o multiThreadSorter_thread -lpthread -lm -g
first:
	gcc multiThreadSorter_thread.c -o multiThreadSorter_thread -lpthread -lm -g
clean:
	$(RM) multiThreadSorter_thread
