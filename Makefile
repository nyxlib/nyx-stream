all:
	mkdir -p ./bin/ && gcc -D_GNU_SOURCE -Wall -O3 -o ./bin/nyx-stream ./src/main.c ./src/mongoose.c && strip ./bin/nyx-stream

clean:
	rm -fr ./bin/
