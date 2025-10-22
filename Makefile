all:
	mkdir -p ./bin/ && gcc -D_GNU_SOURCE -DMG_ENABLE_DIRLIST=0 -DMG_ENABLE_MQTT=1 -DMG_ENABLE_POLL=0 -DMG_ENABLE_EPOLL=1 -DMG_ENABLE_SSI=0 -Wall -Wextra -Wconversion -Wdouble-promotion -O3 -o ./bin/nyx-stream ./src/main.c ./src/external/mongoose.c && strip ./bin/nyx-stream

install:
	cp ./bin/nyx-stream /usr/local/bin/

clean:
	rm -fr ./bin/
