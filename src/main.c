/* NyxStream
 * Author: Jérôme ODIER <jerome.odier@lpsc.in2p3.fr>
 * SPDX-License-Identifier: GPL-2.0-only
 */

/*--------------------------------------------------------------------------------------------------------------------*/

#include <getopt.h>
#include <signal.h>

#include "external/mongoose.h"

/*--------------------------------------------------------------------------------------------------------------------*/
/* CONFIGURATION                                                                                                      */
/*--------------------------------------------------------------------------------------------------------------------*/

static char *HTTP_URL = "http://0.0.0.0:8379";

static char *MQTT_URL = "mqtt://127.0.0.1:1883";

static char *REDIS_URL = "tcp://127.0.0.1:6379";

/*--------------------------------------------------------------------------------------------------------------------*/

static char *MQTT_USERNAME = "";
static char *MQTT_PASSWORD = "";

/*--------------------------------------------------------------------------------------------------------------------*/

static char *REDIS_USERNAME = "";
static char *REDIS_PASSWORD = "";

/*--------------------------------------------------------------------------------------------------------------------*/

static char REDIS_TOKEN[17] = "";

/*--------------------------------------------------------------------------------------------------------------------*/

static uint32_t STREAM_TIMEOUT_MS = 5000U;

static uint32_t KEEPALIVE_MS = 10000U;

static uint32_t POLL_MS = 10U;

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

struct mg_client
{
    struct mg_str stream;

    struct mg_connection *conn;

    struct mg_client *next;
};

/*--------------------------------------------------------------------------------------------------------------------*/

static bool redis_locked = true;

static struct mg_client *clients = NULL;

static struct mg_connection *http_conn = NULL;

static struct mg_connection *mqtt_conn = NULL;

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

    MG_INFO(("Opening stream `%.*s` (ip `%s`)", (int) stream.len, (char *) stream.buf, addr));

    /*----------------------------------------------------------------------------------------------------------------*/
    /* CREATE CLIENT                                                                                                  */
    /*----------------------------------------------------------------------------------------------------------------*/

    struct mg_client *client = (struct mg_client *) malloc(sizeof(struct mg_client));

    if(client == NULL)
    {
        MG_ERROR(("Out of memory!"));

        return;
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    client->stream = stream;
    client->conn = conn;
    client->next = clients;

    /*----------------------------------------------------------------------------------------------------------------*/
    /* REGISTER CLIENT                                                                                                */
    /*----------------------------------------------------------------------------------------------------------------*/

    clients = client;

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void rm_client(const struct mg_connection *conn)
{
    for(struct mg_client **pp = &clients; *pp != NULL; pp = &(*pp)->next)
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

            MG_INFO(("Closing stream `%.*s` (ip `%s`)", (int) (*pp)->stream.len, (char *) (*pp)->stream.buf, addr));

            /*--------------------------------------------------------------------------------------------------------*/

            struct mg_client *dead = *pp; *pp = (*pp)->next;

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
    if(redis_conn == NULL || redis_locked)
    {
        return;
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* SELECT STREAMS                                                                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    size_t sizes = 0;

    size_t n_streams = 0;

    struct mg_str streams[64];

    for(struct mg_client *client = clients; client != NULL && n_streams < sizeof(streams) / sizeof(struct mg_str); client = client->next)
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

        size_t cmd_size = (size_t) snprintf(
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
            cmd_size += (size_t) snprintf(cmd_buff + cmd_size, exp_size - cmd_size, "$%d\r\n%.*s\r\n", (int) streams[i].len, (int) streams[i].len, (char *) streams[i].buf);
        }

        for(size_t i = 0; i < n_streams; i++) {
            cmd_size += (size_t) snprintf(cmd_buff + cmd_size, exp_size - cmd_size, "$1\r\n$\r\n");
        }

        /*------------------------------------------------------------------------------------------------------------*/

        mg_send(redis_conn, cmd_buff, cmd_size);

        redis_locked = true;

        /*------------------------------------------------------------------------------------------------------------*/

        free(cmd_buff);

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

__attribute__ ((always_inline)) static inline char *parse_redis_bulk(struct mg_str *out, char *buf)
{
    /*----------------------------------------------------------------------------------------------------------------*/
    /* EXTRACT SIZE                                                                                                   */
    /*----------------------------------------------------------------------------------------------------------------*/

    buf = strchr(buf, '$');

    if(buf == NULL)
    {
        return NULL;
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    size_t len = strtoul(buf + 1, &buf, 10);

    /*----------------------------------------------------------------------------------------------------------------*/

    if(*(buf + 0) != '\r'
       ||
       *(buf + 1) != '\n'
    ) {
        return NULL;
    }

    buf += 2;

    /*----------------------------------------------------------------------------------------------------------------*/
    /* EXTRACT VALUE                                                                                                  */
    /*----------------------------------------------------------------------------------------------------------------*/

    out->len = len;
    out->buf = buf;

    buf += len;

    /*----------------------------------------------------------------------------------------------------------------*/

    if(*(buf + 0) != '\r'
       ||
       *(buf + 1) != '\n'
    ) {
        return NULL;
    }

    buf += 2;

    /*----------------------------------------------------------------------------------------------------------------*/

    return buf;
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void redis_handler(struct mg_connection *conn, int event, __attribute__ ((unused)) void *event_data)
{
    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_OPEN                                                                                                     */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**/ if(event == MG_EV_OPEN)
    {
        MG_INFO(("%lu REDIS OPEN", conn->id));

        redis_locked = false;

        redis_conn = conn;
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_CLOSE                                                                                                    */
    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_CLOSE)
    {
        MG_INFO(("%lu REDIS CLOSE", conn->id));

        redis_locked = true;

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

            redis_locked = false;

            return;
        }

        /*------------------------------------------------------------------------------------------------------------*/

        struct mg_str stream_name;
        struct mg_str payload;
        struct mg_str temp;

        char *p = (char *) conn->recv.buf;
        char *q = (char *) conn->recv.buf;

        for(;;)
        {
            /*--------------------------------------------------------------------------------------------------------*/
            /* EXTRACT STREAM NAME                                                                                    */
            /*--------------------------------------------------------------------------------------------------------*/

            if((p = parse_redis_bulk(&stream_name, p)) == NULL) goto __exit;

            /*--------------------------------------------------------------------------------------------------------*/
            /* EXTRACT RECORD ID                                                                                      */
            /*--------------------------------------------------------------------------------------------------------*/

            if((p = parse_redis_bulk(&temp, p)) == NULL) goto __exit;

            /*--------------------------------------------------------------------------------------------------------*/
            /* EXTRACT STREAM DIM                                                                                     */
            /*--------------------------------------------------------------------------------------------------------*/

            if((p = strchr(p, '*')) == NULL) goto __exit;
            p += 1;

            uint32_t n_fields = (uint32_t) strtoul(p, &p, 10) / 2;

            if((p = strchr(p, '\r')) == NULL) goto __exit;
            p += 2;

            /*--------------------------------------------------------------------------------------------------------*/
            /* EXTRACT PAYLOAD                                                                                        */
            /*--------------------------------------------------------------------------------------------------------*/

            const char *payload_start = p;

            for(uint32_t i = 0; i < n_fields; i++)
            {
                /*----------------------------------------------------------------------------------------------------*/
                /* EXTRACT FIELD KEY                                                                                  */
                /*----------------------------------------------------------------------------------------------------*/

                if((p = parse_redis_bulk(&temp, p)) == NULL) goto __exit;

                /*----------------------------------------------------------------------------------------------------*/
                /* EXTRACT FIELD VAL                                                                                  */
                /*----------------------------------------------------------------------------------------------------*/

                if((p = parse_redis_bulk(&temp, p)) == NULL) goto __exit;

                /*----------------------------------------------------------------------------------------------------*/
            }

            const char *payload_end = p;

            /*--------------------------------------------------------------------------------------------------------*/

            payload = mg_str_n(payload_start, (size_t) payload_end - (size_t) payload_start);

            /*--------------------------------------------------------------------------------------------------------*/
            /* BUILD MESSAGE                                                                                          */
            /*--------------------------------------------------------------------------------------------------------*/

            size_t message_len = 14 + intlen(n_fields) + payload.len;

            struct mg_str message = mg_str_n(malloc(message_len), message_len);

            if(message.buf == NULL)
            {
                goto __exit;
            }

            memcpy(message.buf + sprintf(message.buf, "nyx-stream[%d]\r\n", n_fields), payload.buf, payload.len);

            /*--------------------------------------------------------------------------------------------------------*/
            /* SEND MESSAGE                                                                                           */
            /*--------------------------------------------------------------------------------------------------------*/

            for(struct mg_client *client = clients; client != NULL; client = client->next)
            {
                if(mg_strcmp(client->stream, stream_name) == 0)
                {
                    mg_ws_send(
                        client->conn,
                        message.buf,
                        message.len,
                        WEBSOCKET_OP_BINARY
                    );
                }
            }

            /*--------------------------------------------------------------------------------------------------------*/
            /* SEND MESSAGE                                                                                           */
            /*--------------------------------------------------------------------------------------------------------*/

            free(message.buf);

            /*--------------------------------------------------------------------------------------------------------*/
        }

        /*------------------------------------------------------------------------------------------------------------*/
__exit:
        mg_iobuf_del(&conn->recv, 0, (size_t) p - (size_t) q);

        redis_locked = false;

        return;

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void mqtt_handler(struct mg_connection *conn, int event, __attribute__ ((unused)) void *event_data)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    /**/ if(event == MG_EV_OPEN)
    {
        MG_INFO(("%lu MQTT OPEN", conn->id));

        mqtt_conn = conn;
    }
    else if(event == MG_EV_CLOSE)
    {
        MG_INFO(("%lu MQTT CLOSE", conn->id));

        mqtt_conn = NULL;
    }
    else if(event == MG_EV_ERROR)
    {
        MG_ERROR(("MQTT ERROR"));
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
        /* AUTHENTICATION                                                                                             */
        /*------------------------------------------------------------------------------------------------------------*/

        if(REDIS_TOKEN[0] != '\0')
        {
            char token_buf[32];

            int token_len = mg_http_get_var(
                &hm->query,
                "token",
                /*--*/(token_buf),
                sizeof(token_buf)
            );

            if(token_len != 16 || memcmp(token_buf, REDIS_TOKEN, 16) != 0)
            {
                mg_http_reply(conn, 403, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Unauthorized\n");

                return;
            }
        }

        /*------------------------------------------------------------------------------------------------------------*/
        /* ROUTE /streams/<device/><stream>                                                                           */
        /*------------------------------------------------------------------------------------------------------------*/

        struct mg_str caps[2];

        /**/ if(mg_match(hm->uri, mg_str("/streams/*/*"), caps) && caps[0].len > 0 && caps[1].len > 0)
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
            mg_http_reply(conn, 200, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "OK\n");

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

static void keepalive_timer_handler(__attribute__ ((unused)) void *arg)
{
    for(struct mg_client *client = clients; client != NULL; client = client->next)
    {
        mg_ws_send(client->conn, "", 0x00, WEBSOCKET_OP_PING);
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
        http_conn = mg_http_listen(mgr, HTTP_URL, http_handler, NULL);

        if(http_conn == NULL)
        {
            MG_ERROR(("Cannot create HTTP listener!"));
        }
        else
        {
            MG_INFO(("Listening on %s", HTTP_URL));
        }
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    if(mqtt_conn == NULL)
    {
        struct mg_mqtt_opts mqtt_opts = {
            .client_id = mg_str("nyx-stream"),
            .user = mg_str(MQTT_USERNAME),
            .pass = mg_str(MQTT_PASSWORD),
            .version = 0x04,
            .clean = true,
        };

        mqtt_conn = mg_mqtt_connect(mgr, MQTT_URL, &mqtt_opts, mqtt_handler, NULL);

        if(mqtt_conn == NULL)
        {
            MG_ERROR(("Cannot open MQTT connection!"));
        }
        else
        {
            MG_INFO(("Connected to %s", MQTT_URL));
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
        else
        {
            MG_INFO(("Connected to %s", REDIS_URL));
        }
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void ping_timer_handler(__attribute__ ((unused)) void *arg)
{
    if(mqtt_conn != NULL)
    {
        struct mg_mqtt_opts opts = {
            .topic = mg_str("nyx/ping/node"),
            .message = mg_str("Nyx Stream Server"),
            .qos = 0,
        };

        mg_mqtt_pub(mqtt_conn, &opts);
    }
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void compute_token(char result[17], const char *username, const char *password)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    uint8_t hash[32];

    mg_sha256_ctx ctx;

    mg_sha256_init(&ctx);
    mg_sha256_update(&ctx, (const uint8_t *) username, strlen(username));
    mg_sha256_update(&ctx, (const uint8_t *) ":", 1);
    mg_sha256_update(&ctx, (const uint8_t *) password, strlen(password));
    mg_sha256_final(hash, &ctx);

    /*----------------------------------------------------------------------------------------------------------------*/

    for(int i = 0; i < 8; i++)
    {
        sprintf(&result[i * 2], "%02x", hash[i]);
    }

    result[16] = '\0';

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void parse_args(int argc, char **argv)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    static struct option long_options[] = {
        {"http",           required_argument, 0, 'h'},
        /**/
        {"mqtt",           required_argument, 0, 'm'},
        {"mqtt-username",  optional_argument, 0, 'u'},
        {"mqtt-password",  optional_argument, 0, 'p'},
        /**/
        {"redis",          required_argument, 0, 'r'},
        {"redis-username", optional_argument, 0, 'v'},
        {"redis-password", optional_argument, 0, 'q'},
        /**/
        {"stream-timeout", required_argument, 0, 's'},
        {"keepalive",      required_argument, 0, 'k'},
        {"poll",           required_argument, 0, 'l'},
        /**/
        {"help",           no_argument,       0, 999},
        /**/
        {0, 0, 0, 0},
    };

    /*----------------------------------------------------------------------------------------------------------------*/

    for(;;)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        int opt = getopt_long(argc, argv, "h:m:u:p:r:v:q:s:k:l:", long_options, NULL);

        if(opt < 0)
        {
            break;
        }

        /*------------------------------------------------------------------------------------------------------------*/

        switch(opt)
        {
            case 'h': HTTP_URL = optarg; break;

            case 'm': MQTT_URL = optarg; break;
            case 'u': MQTT_USERNAME = optarg; break;
            case 'p': MQTT_PASSWORD = optarg; break;

            case 'r': REDIS_URL = optarg; break;
            case 'v': REDIS_USERNAME = optarg; break;
            case 'q': REDIS_PASSWORD = optarg; break;

            case 's': STREAM_TIMEOUT_MS = mg_str_to_uint32(mg_str(optarg), STREAM_TIMEOUT_MS); break;
            case 'k': KEEPALIVE_MS = mg_str_to_uint32(mg_str(optarg), KEEPALIVE_MS); break;
            case 'l': POLL_MS = mg_str_to_uint32(mg_str(optarg), POLL_MS); break;

            default:
                printf("Usage: %s [options]\n", argv[0]);
                printf("  -h --http <url>                 HTTP connection string (default: `%s`)\n", HTTP_URL);
                printf("\n");
                printf("  -m --mqtt <url>                 MQTT connection string (default: `%s`)\n", MQTT_URL);
                printf("  -u --mqtt-username <username>   MQTT username (default: `%s`)\n", MQTT_USERNAME);
                printf("  -p --mqtt-password <password>   MQTT password (default: `%s`)\n", MQTT_PASSWORD);
                printf("\n");
                printf("  -r --redis <url>                Redis connection string (default: `%s`)\n", REDIS_URL);
                printf("  -v --redis-username <username>  Redis username (default: `%s`)\n", REDIS_USERNAME);
                printf("  -q --redis-password <password>  Redis password (default: `%s`)\n", REDIS_PASSWORD);
                printf("\n");
                printf("  -s --stream-timeout <ms>        Stream block timeout (default: %u ms)\n", STREAM_TIMEOUT_MS);
                printf("  -k --keepalive <ms>             Keepalive interval (default: %u ms)\n", KEEPALIVE_MS);
                printf("  -l --poll <ms>                  Poll interval (default: %u ms)\n", POLL_MS);

                exit(0);
        }

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    if(REDIS_USERNAME[0] != '\0'
       ||
       REDIS_PASSWORD[0] != '\0'
    ) {
        compute_token(REDIS_TOKEN, REDIS_USERNAME, REDIS_PASSWORD);
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

int main(int argc, char **argv)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    parse_args(argc, argv);

    /*----------------------------------------------------------------------------------------------------------------*/

    mg_log_set(MG_LL_INFO);

    /*----------------------------------------------------------------------------------------------------------------*/

    MG_INFO(("Starting NyxStream..."));

    /*----------------------------------------------------------------------------------------------------------------*/

    struct mg_mgr mgr;

    mg_mgr_init(&mgr);

    /*----------------------------------------------------------------------------------------------------------------*/

    mg_timer_add(&mgr, KEEPALIVE_MS, MG_TIMER_REPEAT | MG_TIMER_RUN_NOW, keepalive_timer_handler, &mgr);

    mg_timer_add(&mgr, 1000U, MG_TIMER_REPEAT | MG_TIMER_RUN_NOW, retry_timer_handler, &mgr);

    mg_timer_add(&mgr, 5000U, MG_TIMER_REPEAT | MG_TIMER_RUN_NOW, ping_timer_handler, &mgr);

    /*----------------------------------------------------------------------------------------------------------------*/

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

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
