all: scannerCSVsorter.c
	gcc scannerCSVsorter.c -o scannerCSVsorter -lm -g
first:
	gcc scannerCSVsorter.c -o scannerCSVsorter -lm -g
clean:
	$(RM) scannerCSVsorter
