#define MG_ENABLE_CUSTOM_MILLIS 1
#define MG_ENABLE_CUSTOM_DATA 1
#define MG_ENABLE_SSE 1
#include "mongoose.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>

static void *mg_data(struct mg_connection *c) { return c->fn_data; }
static void mg_set_data(struct mg_connection *c, void *data) { c->fn_data = data; }

#define REDIS_HOST       "127.0.0.1"
#define REDIS_PORT       6379
#define REDIS_BLOCK_MS   5000
#define KEEPALIVE_SECS   10.0

/*------------------------------------------------------------------------------------------------*/

typedef struct client_s {
    struct mg_connection *conn;
    char stream[64];
    double last_ping;
    struct client_s *next;
} client_t;

static client_t *clients = NULL;
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

/*------------------------------------------------------------------------------------------------*/

static size_t intlen(size_t n)
{
    size_t len;
    for (len = 1; n >= 10; len++) n /= 10;
    return len;
}

/*------------------------------------------------------------------------------------------------*/

static int redis_query(char **streams, int n_streams, char *buf, size_t bufsize)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(REDIS_PORT),
        .sin_addr.s_addr = inet_addr(REDIS_HOST)
    };

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    char *ptr = buf;
    size_t remaining = bufsize;
    int total = 0;

    total += snprintf(ptr + total, remaining - total,
                      "*%d\r\n"
                      "$5\r\nXREAD\r\n"
                      "$5\r\nBLOCK\r\n"
                      "$%zu\r\n%d\r\n"
                      "$7\r\nSTREAMS\r\n",
                      3 + 2 * n_streams, intlen(REDIS_BLOCK_MS), REDIS_BLOCK_MS);

    for (int i = 0; i < n_streams; ++i) {
        size_t len = strlen(streams[i]);
        total += snprintf(ptr + total, remaining - total, "$%zu\r\n%s\r\n", len, streams[i]);
    }

    for (int i = 0; i < n_streams; ++i) {
        total += snprintf(ptr + total, remaining - total, "$1\r\n$\r\n");
    }

    send(sock, ptr, total, 0);

    int received = recv(sock, buf, bufsize - 1, 0);
    if (received >= 0) buf[received] = '\0';

    close(sock);
    return received;
}

/*------------------------------------------------------------------------------------------------*/

static void add_client(struct mg_connection *c, const char *stream) {
    client_t *cl = calloc(1, sizeof(*cl));
    cl->conn = c;
    snprintf(cl->stream, sizeof(cl->stream), "%s", stream);
    cl->last_ping = mg_millis() / 1000.0;

    pthread_mutex_lock(&clients_mutex);
    cl->next = clients;
    clients = cl;
    pthread_mutex_unlock(&clients_mutex);

    mg_set_data(c, cl);

    mg_printf(c,
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/event-stream\r\n"
        "Cache-Control: no-cache\r\n"
        "Connection: keep-alive\r\n\r\n");
}

static void remove_client(struct mg_connection *c) {
    pthread_mutex_lock(&clients_mutex);
    client_t **pp = &clients;
    while (*pp) {
        if ((*pp)->conn == c) {
            client_t *dead = *pp;
            *pp = (*pp)->next;
            free(dead);
            break;
        }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&clients_mutex);
}

/*------------------------------------------------------------------------------------------------*/

static void broadcast_to_stream(const char *stream, const char *msg) {
    pthread_mutex_lock(&clients_mutex);
    for (client_t *cl = clients; cl; cl = cl->next) {
        if (strcmp(cl->stream, stream) == 0) {
            mg_printf(cl->conn, "data: %s\n\n", msg);
            cl->last_ping = mg_millis() / 1000.0;
        }
    }
    pthread_mutex_unlock(&clients_mutex);
}

/*------------------------------------------------------------------------------------------------*/

static void *redis_thread(void *arg) {
    (void)arg;

    while (1) {
        char *streams[64];
        int n_streams = 0;

        pthread_mutex_lock(&clients_mutex);
        for (client_t *cl = clients; cl && n_streams < 64; cl = cl->next) {
            int exists = 0;
            for (int i = 0; i < n_streams; ++i)
                if (strcmp(streams[i], cl->stream) == 0) exists = 1;
            if (!exists) streams[n_streams++] = cl->stream;
        }
        pthread_mutex_unlock(&clients_mutex);

        if (n_streams == 0) {
            usleep(100 * 1000);
            continue;
        }

        char buf[8192];
        int n = redis_query(streams, n_streams, buf, sizeof(buf));
        if (n <= 0) continue;

        for (int i = 0; i < n_streams; ++i) {
            if (strstr(buf, streams[i])) {
                char *p = strstr(buf, "#samples");
                if (p && (p = strchr(p, '\n')) && (p = strchr(++p, '\n'))) {
                    char *val = ++p;
                    char *end = strchr(val, '\r');
                    if (end) *end = '\0';
                    broadcast_to_stream(streams[i], val);
                }
            }
        }
    }

    return NULL;
}

/*------------------------------------------------------------------------------------------------*/

static bool nyx_startswith(struct mg_str topic, struct mg_str prefix)
{
    return topic.len >= prefix.len && memcmp(topic.buf, prefix.buf, prefix.len) == 0;
}

static void handler(struct mg_connection *c, int ev, void *ev_data) {
    if (ev == MG_EV_HTTP_MSG) {
        struct mg_http_message *hm = (struct mg_http_message *) ev_data;

        if (mg_match(hm->uri, mg_str("/"), NULL)) {
            mg_http_reply(c, 200, "", "OK\n");
            return;
        }

        struct mg_str prefix = mg_str("/streams/");
        if (nyx_startswith(hm->uri, prefix) && hm->uri.len > prefix.len) {
            const char *stream_name = hm->uri.buf + prefix.len;
            size_t stream_len = hm->uri.len - prefix.len;

            char stream[64] = {0};
            snprintf(stream, sizeof(stream), "%.*s", (int)(stream_len > 63 ? 63 : stream_len), stream_name);

            add_client(c, stream);
        } else {
            mg_http_reply(c, 404, "", "Not found\n");
        }
    }

    else if (ev == MG_EV_POLL) {
        client_t *ctx = mg_data(c);
        if (ctx && (mg_millis() / 1000.0 - ctx->last_ping > KEEPALIVE_SECS)) {
            mg_printf(c, ": keepalive\n\n");
            ctx->last_ping = mg_millis() / 1000.0;
        }
    }

    else if (ev == MG_EV_CLOSE) {
        remove_client(c);
    }
}

/*------------------------------------------------------------------------------------------------*/

int main(void) {
    struct mg_mgr mgr;
    pthread_t tid;

    mg_mgr_init(&mgr);
    mg_http_listen(&mgr, "http://0.0.0.0:8007", handler, NULL);
    pthread_create(&tid, NULL, redis_thread, NULL);

    for (;;) mg_mgr_poll(&mgr, 100);
    mg_mgr_free(&mgr);
    return 0;
}

