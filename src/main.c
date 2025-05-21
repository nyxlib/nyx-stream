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

static const char *BIND_URL = "ws://0.0.0.0:8379";

static const char *REDIS_URL = "tcp://127.0.0.1:6379";

/*--------------------------------------------------------------------------------------------------------------------*/

static const char *REDIS_USERNAME = "";
static const char *REDIS_PASSWORD = "";

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

    /*----------------------------------------------------------------------------------------------------------------*/

    struct client_s *next;

    /*----------------------------------------------------------------------------------------------------------------*/

} client_t;

/*--------------------------------------------------------------------------------------------------------------------*/

static client_t *clients = NULL;

static bool volatile redis_waiting = true;

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
    /* CREATE CLIENT                                                                                                  */
    /*----------------------------------------------------------------------------------------------------------------*/

    client_t *client = (client_t *) malloc(sizeof(struct client_s));

    if(client == NULL)
    {
        MG_ERROR(("Out of memory!"));

        return;
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    client->conn   = conn   ;
    client->stream = stream ;
    client->next   = clients;

    /*----------------------------------------------------------------------------------------------------------------*/
    /* REGISTER CLIENT                                                                                                */
    /*----------------------------------------------------------------------------------------------------------------*/

    clients = client;

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

            free(dead);

            break;

            /*--------------------------------------------------------------------------------------------------------*/
        }
    }
}

/*--------------------------------------------------------------------------------------------------------------------*/

void redis_auth()
{
    if(REDIS_PASSWORD[0] != '\0')
    {
        if(REDIS_USERNAME[0] != '\0')
        {
            mg_printf(
                redis_conn,
                "*3\r\n"
                "$4\r\nAUTH\r\n"
                "$%zu\r\n%s\r\n"
                "$%zu\r\n%s\r\n",
                strlen(REDIS_USERNAME),
                /*--*/(REDIS_USERNAME),
                strlen(REDIS_PASSWORD),
                /*--*/(REDIS_PASSWORD)
            );
        }
        else
        {
            mg_printf(
                redis_conn,
                "*2\r\n"
                "$4\r\nAUTH\r\n"
                "$%zu\r\n%s\r\n",
                strlen(REDIS_PASSWORD),
                /*--*/(REDIS_PASSWORD)
            );
        }
    }
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void redis_poll()
{
    if(redis_conn == NULL || redis_waiting)
    {
        return;
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* SELECT STREAMS                                                                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    size_t sizes = 0;

    size_t n_streams = 0;

    struct mg_str streams[64];

    for(client_t *client = clients; client != NULL && n_streams < sizeof(streams) / sizeof(struct mg_str); client = client->next)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        bool found = false;

        for(size_t i = 0; i < n_streams; i++)
        {
            if(mg_strcmp(client->stream, streams[i]) == 0)
            {
                found = true;

                break;
            }
        }

        /*------------------------------------------------------------------------------------------------------------*/

        if(found == false) sizes += 32 + (streams[n_streams++] = client->stream).len;

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* EXECUTE COMMAND                                                                                                */
    /*----------------------------------------------------------------------------------------------------------------*/

    if(n_streams > 0)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        size_t exp_size = 128 + sizes;

        char *cmd_buff = (char *) malloc(exp_size);

        if(cmd_buff == NULL)
        {
            return;
        }

        /*------------------------------------------------------------------------------------------------------------*/

        int cmd_size = snprintf(
            cmd_buff,
            exp_size,
            "*%zu\r\n"
            "$5\r\nXREAD\r\n"
            "$5\r\nBLOCK\r\n"
            "$%zu\r\n%u\r\n"
            "$7\r\nSTREAMS\r\n",
            4 + n_streams * 2,
            intlen(STREAM_TIMEOUT_MS),
            /*--*/(STREAM_TIMEOUT_MS)
        );

        for(size_t i = 0; i < n_streams; i++) {
            cmd_size += snprintf(cmd_buff + cmd_size, exp_size - cmd_size, "$%d\r\n%.*s\r\n", (int) streams[i].len, (int) streams[i].len, streams[i].buf);
        }

        for(size_t i = 0; i < n_streams; i++) {
            cmd_size += snprintf(cmd_buff + cmd_size, exp_size - cmd_size, "$1\r\n$\r\n");
        }

        /*------------------------------------------------------------------------------------------------------------*/

        mg_send(redis_conn, cmd_buff, cmd_size);

        redis_waiting = true;

        /*------------------------------------------------------------------------------------------------------------*/

        free(cmd_buff);

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void redis_handler(struct mg_connection *conn, int event, __attribute__ ((unused)) void *event_data)
{
    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_OPEN                                                                                                  */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**/ if(event == MG_EV_OPEN)
    {
        MG_INFO(("%lu OPEN", conn->id));

        redis_waiting = false;

        redis_conn = conn;
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_CLOSE                                                                                                    */
    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_CLOSE)
    {
        MG_INFO(("%lu CLOSE", conn->id));

        redis_waiting = true;

        redis_conn = NULL;
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_CONNECT                                                                                                  */
    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_CONNECT)
    {
        redis_auth();
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
        struct mg_str stream_dim;
        struct mg_str payload;

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

            const char *stream_dim_start = p + 0;

            p = strstr(p, "\r\n");
            if(p == NULL) goto __exit;
            p += 2;

            const char *stream_dim_end = p - 2;

            /*--------------------------------------------------------------------------------------------------------*/

            stream_dim = mg_str_n(stream_dim_start, (long) stream_dim_end - (long) stream_dim_start);

            /*--------------------------------------------------------------------------------------------------------*/
            /* EXTRACT PAYLOAD                                                                                        */
            /*--------------------------------------------------------------------------------------------------------*/

            uint32_t n_fields = mg_str_to_uint32(stream_dim, 0) / 2;

            /*--------------------------------------------------------------------------------------------------------*/

            const char *payload_start = p;

            for(uint32_t i = 0; i < n_fields; i++)
            {
                /*----------------------------------------------------------------------------------------------------*/
                /* EXTRACT FIELD KEY                                                                                  */
                /*----------------------------------------------------------------------------------------------------*/

                p = strstr(p, "\r\n");
                if(p == NULL) goto __exit;
                p += 2;

                p = strstr(p, "\r\n");
                if(p == NULL) goto __exit;
                p += 2;

                /*----------------------------------------------------------------------------------------------------*/
                /* EXTRACT FIELD VAL                                                                                  */
                /*----------------------------------------------------------------------------------------------------*/

                p = strstr(p, "\r\n");
                if(p == NULL) goto __exit;
                p += 2;

                p = strstr(p, "\r\n");
                if(p == NULL) goto __exit;
                p += 2;

                /*----------------------------------------------------------------------------------------------------*/
            }

            const char *payload_end = p;

            /*--------------------------------------------------------------------------------------------------------*/

            payload = mg_str_n(payload_start, (long) payload_end - (long) payload_start);

            /*--------------------------------------------------------------------------------------------------------*/

            for(client_t *client = clients; client != NULL; client = client->next)
            {
                if(mg_strcmp(client->stream, stream_name) == 0)
                {
                    mg_ws_printf(
                        client->conn,
                        WEBSOCKET_OP_TEXT,
                        "nyx-stream[%.*s]\r\n%.*s",
                        (int) stream_dim.len, stream_dim.buf,
                        (int) payload    .len, payload    .buf
                    );
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
                if(hm->uri.len > 9)
                {
                    conn->fn_data = strndup(
                        hm->uri.buf + 9,
                        hm->uri.len - 9
                    );

                    if(conn->fn_data != NULL)
                    {
                        mg_ws_upgrade(conn, hm, NULL);
                    }
                    else
                    {
                        mg_http_reply(conn, 400, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Out of memory error\n");
                    }
                }
                else
                {
                    mg_http_reply(conn, 400, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Missing stream name\n");
                }
            }
            else
            {
                mg_http_reply(conn, 405, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Method not allowed\n");
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
                mg_http_reply(conn, 405, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Method not allowed\n");
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
                mg_http_reply(conn, 405, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Method not allowed\n");
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
                mg_http_reply(conn, 405, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Method not allowed\n");
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
    /* MG_EV_WS_OPEN                                                                                                  */
    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_WS_OPEN)
    {
        add_client(conn, mg_str(conn->fn_data));
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_CLOSE                                                                                                    */
    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_CLOSE)
    {
        rm_client(conn); free(conn->fn_data);
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void parse_args(int argc, char **argv)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    static struct option long_options[] = {
        {"bind",           required_argument, 0, 'b'},
        {"redis",          required_argument, 0, 'r'},
        {"username",       optional_argument, 0, 'u'},
        {"password",       optional_argument, 0, 'p'},
        {"stream-timeout", required_argument, 0, 's'},
        {"keepalive",      required_argument, 0, 'k'},
        {"poll",           required_argument, 0, 'l'},
        {"help",           no_argument,       0, 'h'},
        {0, 0, 0, 0},
    };

    /*----------------------------------------------------------------------------------------------------------------*/

    for(;;)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        int opt = getopt_long(argc, argv, "b:r:u:p:s:k:l:h", long_options, NULL);

        if(opt < 0)
        {
            break;
        }

        /*------------------------------------------------------------------------------------------------------------*/

        switch(opt)
        {
            case 'b': BIND_URL = optarg; break;
            case 'r': REDIS_URL = optarg; break;

            case 'u': REDIS_USERNAME = optarg; break;
            case 'p': REDIS_PASSWORD = optarg; break;

            case 's': STREAM_TIMEOUT_MS = mg_str_to_uint32(mg_str(optarg), STREAM_TIMEOUT_MS); break;
            case 'k': KEEPALIVE_MS = mg_str_to_uint32(mg_str(optarg), KEEPALIVE_MS); break;
            case 'l': POLL_MS = mg_str_to_uint32(mg_str(optarg), POLL_MS); break;

            case 'h':
            default:
                printf("Usage: %s [options]\n", argv[0]);
                printf("  -b --bind <url>           HTTP connection string (default: `%s`)\n", BIND_URL);
                printf("  -r --redis <url>          Redis connection string (default: `%s`)\n", REDIS_URL);
                printf("\n");
                printf("  -u --username <username>  Redis username (default: `%s`)\n", REDIS_USERNAME);
                printf("  -p --password <password>  Redis password (default: `%s`)\n", REDIS_PASSWORD);
                printf("\n");
                printf("  -s --stream-timeout <ms>  Stream block timeout (default: %u ms)\n", STREAM_TIMEOUT_MS);
                printf("  -k --keepalive <ms>       Keepalive interval (default: %u ms)\n", KEEPALIVE_MS);
                printf("  -l --poll <ms>            Poll interval (default: %u ms)\n", POLL_MS);

                exit(0);
        }

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void keepalive_timer_handler(__attribute__ ((unused)) void *arg)
{
    for(client_t *client = clients; client != NULL; client = client->next)
    {
        mg_ws_send(client->conn, NULL, 0x00, WEBSOCKET_OP_PING);
    }
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void retry_timer_handler(void *arg)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    struct mg_mgr *mgr = (struct mg_mgr *) arg;

    /*----------------------------------------------------------------------------------------------------------------*/

    if(http_conn == NULL)
    {
        http_conn = mg_http_listen(mgr, BIND_URL, http_handler, NULL);

        if(http_conn == NULL)
        {
            MG_ERROR(("Cannot create HTTP listener!"));
        }
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    if(redis_conn == NULL)
    {
        redis_conn = mg_connect(mgr, REDIS_URL, redis_handler, NULL);

        if(redis_conn == NULL)
        {
            MG_ERROR(("Cannot open Redis connection!"));
        }
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

    mg_timer_add(&mgr, KEEPALIVE_MS, MG_TIMER_REPEAT | MG_TIMER_RUN_NOW, keepalive_timer_handler, &mgr);

    mg_timer_add(&mgr, 1000, MG_TIMER_REPEAT | MG_TIMER_RUN_NOW, retry_timer_handler, &mgr);

    /*----------------------------------------------------------------------------------------------------------------*/

    while(s_signo == 0)
    {
        mg_mgr_poll(&mgr, (int) POLL_MS);

        redis_poll();
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    mg_mgr_free(&mgr);

    /*----------------------------------------------------------------------------------------------------------------*/

    MG_INFO(("Bye."));

    return 0;
}

/*--------------------------------------------------------------------------------------------------------------------*/
