all:
	mkdir -p ./bin/ && gcc -D_GNU_SOURCE -Wall -O3 -o ./bin/nyx-stream ./src/main.c ./src/mongoose.c -lpthread

clean:
	rm -fr ./bin/
