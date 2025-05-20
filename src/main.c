/* NyxStream
 * Author: Jérôme ODIER <jerome.odier@lpsc.in2p3.fr>
 * SPDX-License-Identifier: GPL-2.0-only
 */

/*--------------------------------------------------------------------------------------------------------------------*/

#include <getopt.h>
#include <signal.h>

#include "mongoose.h"

/*--------------------------------------------------------------------------------------------------------------------*/
/* CONFIGURATION                                                                                                      */
/*--------------------------------------------------------------------------------------------------------------------*/

static const char *REDIS_URL = "tcp://127.0.0.1:6379";

static const char *BIND_URL = "http://0.0.0.0:8379";

/*--------------------------------------------------------------------------------------------------------------------*/

static uint32_t STREAM_TIMEOUT_MS = 5000;

static uint32_t KEEPALIVE_MS = 10000;

static uint32_t POLL_MS = 10;

/*--------------------------------------------------------------------------------------------------------------------*/
/* HELPERS                                                                                                            */
/*--------------------------------------------------------------------------------------------------------------------*/

static size_t intlen(size_t n)
{
    size_t len;

    for(len = 1; n >= 10; len++)
    {
        n /= 10;
    }

    return len;
}

/*--------------------------------------------------------------------------------------------------------------------*/

static uint32_t mg_str_to_uint32(struct mg_str s, uint32_t default_value)
{
    uint32_t value;

    return s.buf == NULL
           ||
           s.len == 0x00
           ||
           mg_str_to_num(s, 10, &value, sizeof(value)) == false ? default_value : value
    ;
}

/*--------------------------------------------------------------------------------------------------------------------*/
/* SERVER                                                                                                             */
/*--------------------------------------------------------------------------------------------------------------------*/

static int volatile s_signo = 0;

static void signal_handler(int signo)
{
    s_signo = signo;
}

/*--------------------------------------------------------------------------------------------------------------------*/

typedef struct client_s
{
    /*----------------------------------------------------------------------------------------------------------------*/

    struct mg_connection *conn;

    struct mg_str stream;

    uint64_t last_ping;

    /*----------------------------------------------------------------------------------------------------------------*/

    struct client_s *next;

    /*----------------------------------------------------------------------------------------------------------------*/

} client_t;

/*--------------------------------------------------------------------------------------------------------------------*/

static client_t *clients = NULL;

static bool volatile redis_waiting = false;

static struct mg_connection *http_conn = NULL;

static struct mg_connection *redis_conn = NULL;

/*--------------------------------------------------------------------------------------------------------------------*/

static void add_client(struct mg_connection *conn, struct mg_str stream)
{
   /*-----------------------------------------------------------------------------------------------------------------*/

    char addr[INET6_ADDRSTRLEN] = {0};

    if(conn->rem.is_ip6) {
        inet_ntop(AF_INET6, &conn->rem.ip, addr, sizeof(addr));
    } else {
        inet_ntop(AF_INET, &conn->rem.ip, addr, sizeof(addr));
    }

    MG_INFO(("Opening stream `%.*s` (ip `%s`)", (int) stream.len, stream.buf, addr));

    /*----------------------------------------------------------------------------------------------------------------*/

    client_t *client = (client_t *) malloc(sizeof(struct client_s));

    client->conn      = conn             ;
    client->stream    = mg_strdup(stream);
    client->last_ping = mg_millis()      ;
    client->next      = clients          ;

    /*----------------------------------------------------------------------------------------------------------------*/

    conn->fn_data = clients = client;

    /*----------------------------------------------------------------------------------------------------------------*/

    mg_printf(
        conn,
        "HTTP/1.1 200 OK\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Content-Type: text/event-stream\r\n"
        "Cache-Control: no-cache\r\n"
        "Connection: keep-alive\r\n"
        "\r\n"
    );

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void rm_client(struct mg_connection *conn)
{
    for(client_t **pp = &clients; *pp != NULL; pp = &(*pp)->next)
    {
        if((*pp)->conn == conn)
        {
            /*--------------------------------------------------------------------------------------------------------*/

            char addr[INET6_ADDRSTRLEN] = {0};

            if(conn->rem.is_ip6) {
                inet_ntop(AF_INET6, &conn->rem.ip, addr, sizeof(addr));
            } else {
                inet_ntop(AF_INET, &conn->rem.ip, addr, sizeof(addr));
            }

            MG_INFO(("Closing stream `%.*s` (ip `%s`)", (int) (*pp)->stream.len, (*pp)->stream.buf, addr));

            /*--------------------------------------------------------------------------------------------------------*/

            client_t *dead = *pp; *pp = (*pp)->next;

            free(dead->stream.buf);

            free(dead);

            break;

            /*--------------------------------------------------------------------------------------------------------*/
        }
    }
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void redis_poll(struct mg_connection *redis_conn)
{
    if(redis_conn == NULL || redis_waiting)
    {
        return;
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* SELECT STREAMS                                                                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    int n_streams = 0;

    struct mg_str streams[64];

    for(client_t *client = clients; client != NULL && n_streams < sizeof(streams) / sizeof(struct mg_str); client = client->next)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        bool found = false;

        for(int i = 0; i < n_streams; i++)
        {
            if(mg_strcmp(client->stream, streams[i]) == 0)
            {
                found = true;

                break;
            }
        }

        /*------------------------------------------------------------------------------------------------------------*/

        if(found == false) streams[n_streams++] = client->stream;

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* EXECUTE COMMAND                                                                                                */
    /*----------------------------------------------------------------------------------------------------------------*/

    if(n_streams > 0)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        char cmd[8192];

        int len = snprintf(
            /*--*/(cmd),
            sizeof(cmd),
            "*%d\r\n"
            "$5\r\nXREAD\r\n"
            "$5\r\nBLOCK\r\n"
            "$%zu\r\n%u\r\n"
            "$7\r\nSTREAMS\r\n",
            4 + n_streams * 2,
            intlen(STREAM_TIMEOUT_MS),
            /*--*/(STREAM_TIMEOUT_MS)
        );

        for(int i = 0; i < n_streams; i++) {
            len += snprintf(cmd + len, sizeof(cmd) - len, "$%d\r\n%.*s\r\n", (int) streams[i].len, (int) streams[i].len, streams[i].buf);
        }

        for(int i = 0; i < n_streams; i++) {
            len += snprintf(cmd + len, sizeof(cmd) - len, "$1\r\n$\r\n");
        }

        /*------------------------------------------------------------------------------------------------------------*/

        mg_send(redis_conn, cmd, len);

        redis_waiting = true;

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void redis_handler(struct mg_connection *conn, int event, __attribute__ ((unused)) void *event_data)
{
    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_CONNECT                                                                                                  */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**/ if(event == MG_EV_CONNECT)
    {
        MG_INFO(("%lu OPEN", conn->id));

        /* Redis is up */
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_CLOSE                                                                                                    */
    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_CLOSE)
    {
        MG_INFO(("%lu CLOSE", conn->id));

        redis_conn = NULL;
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_READ                                                                                                     */
    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_READ)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        if(conn->recv.len >= 5 && memcmp(conn->recv.buf, "*-1\r\n", 5) == 0)
        {
            mg_iobuf_del(&conn->recv, 0, 5);

            redis_waiting = false;

            return;
        }

        /*------------------------------------------------------------------------------------------------------------*/

        struct mg_str stream_name;
        struct mg_str stream_size;

        char *p = (char *) conn->recv.buf;
        char *q = (char *) conn->recv.buf;

        for(;;)
        {
            /*--------------------------------------------------------------------------------------------------------*/
            /* EXTRACT STREAM NAME                                                                                    */
            /*--------------------------------------------------------------------------------------------------------*/

            p = strchr(p, '$');
            if(p == NULL) goto __exit;
            p += 1;

            p = strstr(p, "\r\n");
            if(p == NULL) goto __exit;
            p += 2;

            const char *stream_name_start = p + 0;

            p = strstr(p, "\r\n");
            if(p == NULL) goto __exit;
            p += 2;

            const char *stream_name_end = p - 2;

            /*--------------------------------------------------------------------------------------------------------*/

            stream_name = mg_str_n(stream_name_start, (long) stream_name_end - (long) stream_name_start);

            /*--------------------------------------------------------------------------------------------------------*/
            /* EXTRACT STREAM DIM                                                                                     */
            /*--------------------------------------------------------------------------------------------------------*/

            p = strchr(p, '$');
            if(p == NULL) goto __exit;
            p += 1;

            p = strchr(p, '*');
            if(p == NULL) goto __exit;
            p += 1;

            const char *stream_size_start = p + 0;

            p = strstr(p, "\r\n");
            if(p == NULL) goto __exit;
            p += 2;

            const char *stream_size_end = p - 2;

            /*--------------------------------------------------------------------------------------------------------*/

            stream_size = mg_str_n(stream_size_start, (long) stream_size_end - (long) stream_size_start);

            /*--------------------------------------------------------------------------------------------------------*/
            /* EXTRACT AND SEND PAYLOAD                                                                               */
            /*--------------------------------------------------------------------------------------------------------*/

            for(client_t *client = clients; client != NULL; client = client->next)
            {
                if(mg_strcmp(client->stream, stream_name) == 0)
                {
                    mg_printf(client->conn, "data: {");
                }
            }

            /*--------------------------------------------------------------------------------------------------------*/

            uint32_t n_fields = mg_str_to_uint32(stream_size, 0) / 2;

            /*--------------------------------------------------------------------------------------------------------*/

            for(uint32_t i = 0; i < n_fields; i++)
            {
                /*----------------------------------------------------------------------------------------------------*/
                /* EXTRACT FIELD KEY                                                                                  */
                /*----------------------------------------------------------------------------------------------------*/

                p = strstr(p, "\r\n");
                if(p == NULL) goto __exit;
                p += 2;

                const char *key_start = p + 0;

                p = strstr(p, "\r\n");
                if(p == NULL) goto __exit;
                p += 2;

                const char *key_end = p - 2;

                /*----------------------------------------------------------------------------------------------------*/

                int key_len = (int) ((long) key_end - (long) key_start);

                /*----------------------------------------------------------------------------------------------------*/
                /* EXTRACT FIELD VAL                                                                                  */
                /*----------------------------------------------------------------------------------------------------*/

                p = strstr(p, "\r\n");
                if(p == NULL) goto __exit;
                p += 2;

                const char *val_start = p + 0;

                p = strstr(p, "\r\n");
                if(p == NULL) goto __exit;
                p += 2;

                const char *val_end = p - 2;

                /*----------------------------------------------------------------------------------------------------*/

                int val_len = (int) ((long) val_end - (long) val_start);

                /*----------------------------------------------------------------------------------------------------*/
                /* EMIT JSON KEY-VAL ENTRY                                                                            */
                /*----------------------------------------------------------------------------------------------------*/

                for(client_t *client = clients; client != NULL; client = client->next)
                {
                    if(mg_strcmp(client->stream, stream_name) == 0)
                    {
                        if(i == n_fields - 1) {
                            mg_printf(client->conn, "\"%.*s\":\"%.*s\"", (int) key_len, key_start, (int) val_len, val_start);
                        }
                        else {
                            mg_printf(client->conn, "\"%.*s\":\"%.*s\",", (int) key_len, key_start, (int) val_len, val_start);
                        }
                    }
                }

                /*----------------------------------------------------------------------------------------------------*/
            }

            /*--------------------------------------------------------------------------------------------------------*/

            for(client_t *client = clients; client != NULL; client = client->next)
            {
                if(mg_strcmp(client->stream, stream_name) == 0)
                {
                    mg_printf(client->conn, "}\n\n");

                    client->last_ping = mg_millis();
                }
            }

            /*--------------------------------------------------------------------------------------------------------*/
        }

        /*------------------------------------------------------------------------------------------------------------*/
__exit:
        mg_iobuf_del(&conn->recv, 0, (long) p - (long) q);

        redis_waiting = false;

        return;

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void http_handler(struct mg_connection *conn, int event, void *event_data)
{
    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_MQTT_MSG                                                                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**/ if(event == MG_EV_HTTP_MSG)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        struct mg_http_message *hm = (struct mg_http_message *) event_data;

        /*------------------------------------------------------------------------------------------------------------*/
        /* ROUTE /streams/<stream>                                                                                    */
        /*------------------------------------------------------------------------------------------------------------*/

        /**/ if(mg_match(hm->uri, mg_str("/streams/*"), NULL))
        {
            if(mg_strcasecmp(hm->method, mg_str("GET")) == 0)
            {
                add_client(conn, mg_str_n(
                    hm->uri.buf + 9,
                    hm->uri.len - 9
                ));
            }
            else
            {
                mg_http_reply(conn, 405, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Method Not Allowed\n");
            }
        }

        /*------------------------------------------------------------------------------------------------------------*/
        /* ROUTE /config/stream-timeout                                                                               */
        /*------------------------------------------------------------------------------------------------------------*/

        else if(mg_match(hm->uri, mg_str("/config/stream-timeout"), NULL))
        {
            /**/ if(mg_strcasecmp(hm->method, mg_str("POST")) == 0) {
                mg_http_reply(conn, 200, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "%u\n", STREAM_TIMEOUT_MS = mg_str_to_uint32(hm->body, STREAM_TIMEOUT_MS));
            }
            else if(mg_strcasecmp(hm->method, mg_str("GET")) == 0) {
                mg_http_reply(conn, 200, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "%u\n", /*------*/ STREAM_TIMEOUT_MS /*------*/);
            }
            else {
                mg_http_reply(conn, 405, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Method Not Allowed\n");
            }
        }

        /*------------------------------------------------------------------------------------------------------------*/
        /* ROUTE /config/keepalive                                                                                    */
        /*------------------------------------------------------------------------------------------------------------*/

        else if(mg_match(hm->uri, mg_str("/config/keepalive"), NULL))
        {
            /**/ if(mg_strcasecmp(hm->method, mg_str("POST")) == 0) {
                mg_http_reply(conn, 200, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "%u\n", KEEPALIVE_MS = mg_str_to_uint32(hm->body, KEEPALIVE_MS));
            }
            else if(mg_strcasecmp(hm->method, mg_str("GET")) == 0) {
                mg_http_reply(conn, 200, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "%u\n", /*------*/ KEEPALIVE_MS /*------*/);
            }
            else {
                mg_http_reply(conn, 405, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Method Not Allowed\n");
            }
        }

        /*------------------------------------------------------------------------------------------------------------*/
        /* ROUTE /config/poll                                                                                         */
        /*------------------------------------------------------------------------------------------------------------*/

        else if(mg_match(hm->uri, mg_str("/config/poll"), NULL))
        {
            /**/ if(mg_strcasecmp(hm->method, mg_str("POST")) == 0) {
                mg_http_reply(conn, 200, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "%u\n", POLL_MS = mg_str_to_uint32(hm->body, POLL_MS));
            }
            else if(mg_strcasecmp(hm->method, mg_str("GET")) == 0) {
                mg_http_reply(conn, 200, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "%u\n", /*------*/ POLL_MS /*------*/);
            }
            else {
                mg_http_reply(conn, 405, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Method Not Allowed\n");
            }
        }

        /*------------------------------------------------------------------------------------------------------------*/
        /* ROUTE /stop                                                                                                */
        /*------------------------------------------------------------------------------------------------------------*/

        else if(mg_match(hm->uri, mg_str("/stop"), NULL))
        {
            s_signo = 1;
        }

        /*------------------------------------------------------------------------------------------------------------*/
        /* ROUTE *                                                                                                    */
        /*------------------------------------------------------------------------------------------------------------*/

        else
        {
            mg_http_reply(conn, 404, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n",
                "/streams/<stream> [GET]\n"
                "/config/stream-timeout [GET, POST]\n"
                "/config/keepalive [GET, POST]\n"
                "/config/poll [GET, POST]\n"
                "/stop [GET, POST]\n"
            );
        }

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_CLOSE                                                                                                    */
    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_CLOSE)
    {
        rm_client(conn);
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_POLL                                                                                                     */
    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_POLL)
    {
        client_t *client = (client_t *) conn->fn_data;

        if(client != NULL && (mg_millis() - client->last_ping) > KEEPALIVE_MS)
        {
            mg_printf(conn, ": keepalive\n\n");

            client->last_ping = mg_millis();
        }
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void parse_args(int argc, char **argv)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    static struct option long_options[] = {
        {"redis",          required_argument, 0, 'r'},
        {"bind",           required_argument, 0, 'b'},
        {"stream-timeout", required_argument, 0, 's'},
        {"keepalive",      required_argument, 0, 'k'},
        {"poll",           required_argument, 0, 'p'},
        {"help",           no_argument,       0, 'h'},
        {0, 0, 0, 0},
    };

    /*----------------------------------------------------------------------------------------------------------------*/

    for(;;)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        int opt = getopt_long(argc, argv, "r:b:s:k:p:h", long_options, NULL);

        if(opt < 0)
        {
            break;
        }

        /*------------------------------------------------------------------------------------------------------------*/

        switch(opt)
        {
            case 'r': REDIS_URL  = optarg; break;
            case 'b': BIND_URL = optarg; break;

            case 's': STREAM_TIMEOUT_MS = mg_str_to_uint32(mg_str(optarg), STREAM_TIMEOUT_MS); break;
            case 'k': KEEPALIVE_MS = mg_str_to_uint32(mg_str(optarg), KEEPALIVE_MS); break;
            case 'p': POLL_MS = mg_str_to_uint32(mg_str(optarg), POLL_MS); break;

            case 'h':
            default:
                printf("Usage: %s [options]\n", argv[0]);
                printf("  --redis <url>          Redis connection string (default: %s)\n", REDIS_URL);
                printf("  --bind <url>           HTTP connection string (default: %s)\n", BIND_URL);
                printf("\n");
                printf("  --stream-timeout <ms>  Stream block timeout (default: %u ms)\n", STREAM_TIMEOUT_MS);
                printf("  --keepalive <ms>       Keepalive interval (default: %u ms)\n", KEEPALIVE_MS);
                printf("  --poll <ms>            Poll interval (default: %u ms)\n", POLL_MS);

                exit(0);
        }

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void retry_timer_handler(void *arg)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    struct mg_mgr *mgr = (struct mg_mgr *) arg;

    /*----------------------------------------------------------------------------------------------------------------*/

    http_conn = mg_http_listen(mgr, BIND_URL, http_handler, NULL);

    if(http_conn == NULL)
    {
        MG_INFO(("Cannot create HTTP listener!"));
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    redis_conn = mg_connect(mgr, REDIS_URL, redis_handler, NULL);

    if(redis_conn == NULL)
    {
        MG_INFO(("Cannot open Redis connection!"));
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

int main(int argc, char **argv)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    parse_args(argc, argv);

    /*----------------------------------------------------------------------------------------------------------------*/

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    /*----------------------------------------------------------------------------------------------------------------*/

    MG_INFO(("Starting NyxStream on %s, Redis at %s...", BIND_URL, REDIS_URL));

    /*----------------------------------------------------------------------------------------------------------------*/

    struct mg_mgr mgr;

    mg_mgr_init(&mgr);

    /*----------------------------------------------------------------------------------------------------------------*/

    mg_timer_add(&mgr, 1000, MG_TIMER_REPEAT | MG_TIMER_RUN_NOW, retry_timer_handler, &mgr);

    /*----------------------------------------------------------------------------------------------------------------*/

    do {
        mg_mgr_poll(&mgr, (int) POLL_MS);

        redis_poll(redis_conn);

    } while(s_signo == 0);

    /*----------------------------------------------------------------------------------------------------------------*/

    mg_mgr_free(&mgr);

    /*----------------------------------------------------------------------------------------------------------------*/

    MG_INFO(("Bye."));

    return 0;
}

/*--------------------------------------------------------------------------------------------------------------------*/
