all: split

split: arrange.c hash.c
	gcc hash.c arrange.c -std=gnu99 -O3 -o split
