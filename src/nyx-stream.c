/* NyxStream
 * Author: Jérôme ODIER <jerome.odier@lpsc.in2p3.fr>
 * SPDX-License-Identifier: GPL-2.0-only
 */

/*--------------------------------------------------------------------------------------------------------------------*/

#include <getopt.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

#include "nyx-stream.h"

#include "external/mongoose.h"

/*--------------------------------------------------------------------------------------------------------------------*/

static str_t TCP_URL  = "tcp://0.0.0.0:8888";

static str_t HTTP_URL = "http://0.0.0.0:9999";

static str_t MQTT_URL = "mqtt://127.0.0.1:1883";

/*--------------------------------------------------------------------------------------------------------------------*/

static str_t MQTT_USERNAME = "";
static str_t MQTT_PASSWORD = "";

/*--------------------------------------------------------------------------------------------------------------------*/

static char TOKEN[17] = "";

/*--------------------------------------------------------------------------------------------------------------------*/

static uint32_t POLL_MS = 10U;

/*--------------------------------------------------------------------------------------------------------------------*/

#define KEEPALIVE_MS 10000U

#define RETRY_MS 1000U

#define PING_MS 5000U

/*--------------------------------------------------------------------------------------------------------------------*/
/* UTILITIES                                                                                                          */
/*--------------------------------------------------------------------------------------------------------------------*/

static uint32_t mg_str_to_uint32(const struct mg_str s, const uint32_t default_value)
{
    uint32_t parsed_value;

    return s.len != 0x00
           &&
           s.buf != NULL
           &&
           mg_str_to_num(s, 10, &parsed_value, sizeof(parsed_value)) ? parsed_value : default_value
    ;
}

/*--------------------------------------------------------------------------------------------------------------------*/
/* SIGNAL                                                                                                             */
/*--------------------------------------------------------------------------------------------------------------------*/

static volatile sig_atomic_t s_signo = 0;

/*--------------------------------------------------------------------------------------------------------------------*/

static void signal_handler(const int signo)
{
    s_signo = signo;
}

/*--------------------------------------------------------------------------------------------------------------------*/
/* SERVER                                                                                                             */
/*--------------------------------------------------------------------------------------------------------------------*/

#define STREAM_MAGIC 0x5358594EU

#define STREAM_HEADER_SIZE (4U /* MAGIC */ + 4U /* HASH */ + 4U /* SIZE */)

/*--------------------------------------------------------------------------------------------------------------------*/

struct mg_client
{
    uint32_t hash;

    uint32_t period_ms;
    uint64_t last_send_ms;

    struct mg_connection *conn;

    struct mg_client *next;
};

/*--------------------------------------------------------------------------------------------------------------------*/

static struct mg_client *clients = NULL;

/*--------------------------------------------------------------------------------------------------------------------*/

static struct mg_connection *tcp_conn = NULL;

static struct mg_connection *http_conn = NULL;

static struct mg_connection *mqtt_conn = NULL;

/*--------------------------------------------------------------------------------------------------------------------*/
/* CLIENT MANAGEMENT                                                                                                  */
/*--------------------------------------------------------------------------------------------------------------------*/

static void add_client(struct mg_connection *conn, const struct mg_str stream, const uint32_t period_ms)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    const uint32_t hash = nyx_hash(stream.len, stream.buf, STREAM_MAGIC);

    /*----------------------------------------------------------------------------------------------------------------*/

    char addr[INET6_ADDRSTRLEN] = {0};

    if(conn->rem.is_ip6) {
        inet_ntop(AF_INET6, &conn->rem.ip, addr, sizeof(addr));
    } else {
        inet_ntop(AF_INET, &conn->rem.ip, addr, sizeof(addr));
    }

    MG_INFO(("Opening stream %08X (name: `%.*s`, period %u ms, ip `%s`)", hash, (int) stream.len, (str_t) stream.buf, period_ms, addr));

    /*----------------------------------------------------------------------------------------------------------------*/
    /* CREATE CLIENT                                                                                                  */
    /*----------------------------------------------------------------------------------------------------------------*/

    struct mg_client *client = nyx_memory_alloc(sizeof(struct mg_client));

    memset(client, 0x00, sizeof(struct mg_client));

    /*----------------------------------------------------------------------------------------------------------------*/

    client->hash = hash;

    client->period_ms = period_ms;
    client->last_send_ms = 0x0000LLU;

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

            MG_INFO(("Closing stream `%08X` (ip `%s`)", (*pp)->hash, addr));

            /*--------------------------------------------------------------------------------------------------------*/

            struct mg_client *dead = *pp; *pp = (*pp)->next;

            nyx_memory_free(dead);

            break;

            /*--------------------------------------------------------------------------------------------------------*/
        }
    }
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void tcp_handler(struct mg_connection *conn, int event, __NYX_UNUSED__ void *event_data)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    /**/ if(event == MG_EV_OPEN)
    {
        MG_INFO(("%lu TCP OPEN", conn->id));
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_CLOSE)
    {
        MG_INFO(("%lu TCP CLOSE", conn->id));
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    else if(event == MG_EV_READ)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        struct mg_iobuf *iobuf = &conn->recv;

        /*------------------------------------------------------------------------------------------------------------*/

        size_t off = 0U;

        while(iobuf->len - off >= STREAM_HEADER_SIZE)
        {
            /*--------------------------------------------------------------------------------------------------------*/

            const uint8_t *frame_buff = (const uint8_t *) iobuf->buf + off;

            /*--------------------------------------------------------------------------------------------------------*/

            const uint32_t magic = nyx_read_u32_le(frame_buff + 0);
            const uint32_t stream_hash = nyx_read_u32_le(frame_buff + 4);
            const uint32_t payload_size = nyx_read_u32_le(frame_buff + 8);

            if(magic != STREAM_MAGIC)
            {
                off += 1U;

                continue;
            }

            /*--------------------------------------------------------------------------------------------------------*/

            const size_t frame_size = STREAM_HEADER_SIZE + (size_t) payload_size;

            if(iobuf->len - off < frame_size)
            {
                /* Incomplete frame, wait... */

                break;
            }

            /*--------------------------------------------------------------------------------------------------------*/

            if(payload_size > 0U)
            {
                /*----------------------------------------------------------------------------------------------------*/

                const uint64_t now = mg_millis();

                /*----------------------------------------------------------------------------------------------------*/

                for(struct mg_client *client = clients; client != NULL; client = client->next)
                {
                    if(client->hash == stream_hash && (client->period_ms == 0U || (now - client->last_send_ms) >= (uint64_t) client->period_ms))
                    {
                        mg_ws_send(
                            client->conn,
                            frame_buff,
                            frame_size,
                            WEBSOCKET_OP_BINARY
                        );

                        client->last_send_ms = now;
                    }
                }

                /*----------------------------------------------------------------------------------------------------*/
            }

            /*--------------------------------------------------------------------------------------------------------*/

            off += frame_size;

            /*--------------------------------------------------------------------------------------------------------*/
        }

        /*------------------------------------------------------------------------------------------------------------*/

        if(off > 0U)
        {
            mg_iobuf_del(iobuf, 0U, off);
        }

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void http_handler(struct mg_connection *conn, int event, void *event_data)
{
    /*----------------------------------------------------------------------------------------------------------------*/
    /* MG_EV_HTTP_MSG                                                                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**/ if(event == MG_EV_HTTP_MSG)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        struct mg_http_message *hm = event_data;

        /*------------------------------------------------------------------------------------------------------------*/
        /* AUTHENTICATION                                                                                             */
        /*------------------------------------------------------------------------------------------------------------*/

        if(TOKEN[0] != '\0')
        {
            char token_buf[32];

            const int token_len = mg_http_get_var(
                &hm->query,
                "token",
                /*--*/(token_buf),
                sizeof(token_buf)
            );

            if(token_len != 16 || memcmp(token_buf, TOKEN, 16) != 0)
            {
                mg_http_reply(conn, 403, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n", "Unauthorized\n");

                return;
            }
        }

        /*------------------------------------------------------------------------------------------------------------*/
        /* ROUTE /streams/<device>/<stream>                                                                           */
        /*------------------------------------------------------------------------------------------------------------*/

        struct mg_str caps[2];

        /**/ if(mg_match(hm->uri, mg_str("/streams/*/*"), caps) && caps[0].len > 0 && caps[1].len > 0)
        {
            if(mg_strcasecmp(hm->method, mg_str("GET")) == 0)
            {
                /*----------------------------------------------------------------------------------------------------*/

                char period_buf[16];

                const int period_len = mg_http_get_var(
                    &hm->query,
                    "period",
                    /*--*/(period_buf),
                    sizeof(period_buf)
                );

                if(period_len > 0)
                {
                    memcpy(conn->data, period_buf, (size_t) period_len + 1);
                }
                else
                {
                    *(str_t) conn->data = '\0';
                }

                /*----------------------------------------------------------------------------------------------------*/

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
            mg_http_reply(conn, 200, "Access-Control-Allow-Origin: *\r\nContent-Type: text/plain\r\n",
                "/streams/<device>/<stream>?period=<ms> [GET]\n"
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
        /*------------------------------------------------------------------------------------------------------------*/

        uint32_t period_ms = 0U;

        if(*(str_t) conn->data != '\0')
        {
            period_ms = mg_str_to_uint32(mg_str(conn->data), 0U);
        }

        /*------------------------------------------------------------------------------------------------------------*/

        add_client(conn, mg_str(conn->fn_data), period_ms);

        /*------------------------------------------------------------------------------------------------------------*/
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

static void mqtt_handler(struct mg_connection *conn, int event, __NYX_UNUSED__ void *event_data)
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

static void retry_timer_handler(void *arg)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    struct mg_mgr *mgr = arg;

    /*----------------------------------------------------------------------------------------------------------------*/

    if(tcp_conn == NULL)
    {
        tcp_conn = mg_listen(mgr, TCP_URL, tcp_handler, NULL);

        if(tcp_conn == NULL)
        {
            MG_ERROR(("Cannot create TCP listener!"));
        }
        else
        {
            MG_INFO(("TCP listening on %s", TCP_URL));
        }
    }

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
            MG_INFO(("HTTP listening on %s", HTTP_URL));
        }
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    if(mqtt_conn == NULL)
    {
        const struct mg_mqtt_opts mqtt_opts = {
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
            MG_INFO(("MQTT connected to %s", MQTT_URL));
        }
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void keepalive_timer_handler(__NYX_UNUSED__ void *arg)
{
    for(const struct mg_client *client = clients; client != NULL; client = client->next)
    {
        mg_ws_send(client->conn, "", 0x00, WEBSOCKET_OP_PING);
    }
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void ping_timer_handler(__NYX_UNUSED__ void *arg)
{
    if(mqtt_conn != NULL)
    {
        const struct mg_mqtt_opts opts = {
            .topic = mg_str("nyx/ping/special"),
            .message = mg_str("$$nyx-stream-server$$"),
            .qos = 0,
        };

        mg_mqtt_pub(mqtt_conn, &opts);
    }
}

/*--------------------------------------------------------------------------------------------------------------------*/

static void compute_token(char result[17], STR_t username, STR_t password)
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

static void parse_args(const int argc, str_t *argv)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    str_t tcp_url;
    str_t http_url;
    str_t mqtt_url;
    str_t mqtt_username;
    str_t mqtt_password;
    str_t poll_ms;

    if(nyx_load_config(
        &tcp_url,
        &http_url,
        &mqtt_url,
        &mqtt_username,
        &mqtt_password,
        &poll_ms
    )) {
        if(tcp_url != NULL) TCP_URL = tcp_url;
        if(http_url != NULL) HTTP_URL = http_url;
        if(mqtt_url != NULL) MQTT_URL = mqtt_url;

        if(mqtt_username != NULL) MQTT_USERNAME = mqtt_username;
        if(mqtt_password != NULL) MQTT_PASSWORD = mqtt_password;

        if(poll_ms != NULL) mg_str_to_uint32(mg_str(optarg), POLL_MS);
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    static struct option long_options[] = {
        {"tcp-url",  required_argument, 0, 't'},
        {"http-url", required_argument, 0, 'h'},
        {"mqtt-url", required_argument, 0, 'm'},
        /**/
        {"username", optional_argument, 0, 'u'},
        {"password", optional_argument, 0, 'p'},
        /**/
        {"poll",     required_argument, 0, 'l'},
        /**/
        {"help",     no_argument,       0, 999},
        /**/
        {0, 0, 0, 0},
    };

    /*----------------------------------------------------------------------------------------------------------------*/

    for(;;)
    {
        /*------------------------------------------------------------------------------------------------------------*/

        const int opt = getopt_long(argc, argv, "t:h:m:u:p:l:", long_options, NULL);

        if(opt < 0)
        {
            break;
        }

        /*------------------------------------------------------------------------------------------------------------*/

        switch(opt)
        {
            case 't': TCP_URL       = optarg; break;
            case 'h': HTTP_URL      = optarg; break;
            case 'm': MQTT_URL      = optarg; break;

            case 'u': MQTT_USERNAME = optarg; break;
            case 'p': MQTT_PASSWORD = optarg; break;

            case 'l': POLL_MS       = mg_str_to_uint32(mg_str(optarg), POLL_MS); break;

            default:
                printf("Usage: %s [options]\n", argv[0]);
                printf("\n");
                printf("  -t --tcp-url <url>        TCP connection string (default: `%s`)\n", TCP_URL);
                printf("  -h --http-url <url>       HTTP connection string (default: `%s`)\n", HTTP_URL);
                printf("  -m --mqtt-url <url>       MQTT connection string (default: `%s`)\n", MQTT_URL);
                printf("\n");
                printf("  -u --username <username>  Username for both HTTP and MQTT\n");
                printf("  -p --password <password>  Password for both HTTP and MQTT\n");
                printf("\n");
                printf("  -l --poll <ms>            Poll interval (default: %u ms)\n", POLL_MS);

                exit(0);
        }

        /*------------------------------------------------------------------------------------------------------------*/
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    if(MQTT_USERNAME[0] != '\0'
       ||
       MQTT_PASSWORD[0] != '\0'
    ) {
        compute_token(TOKEN, MQTT_USERNAME, MQTT_PASSWORD);
    }

    /*----------------------------------------------------------------------------------------------------------------*/
}

/*--------------------------------------------------------------------------------------------------------------------*/

int main(const int argc, str_t *argv)
{
    /*----------------------------------------------------------------------------------------------------------------*/

    parse_args(argc, argv);

    /*----------------------------------------------------------------------------------------------------------------*/

    mg_log_set(MG_LL_INFO);

    /*----------------------------------------------------------------------------------------------------------------*/

    MG_INFO(("Starting Nyx-Stream..."));

    /*----------------------------------------------------------------------------------------------------------------*/

    struct mg_mgr mgr;

    mg_mgr_init(&mgr);

    /*----------------------------------------------------------------------------------------------------------------*/

    mg_timer_add(&mgr, KEEPALIVE_MS, MG_TIMER_REPEAT | MG_TIMER_RUN_NOW, keepalive_timer_handler, &mgr);

    mg_timer_add(&mgr, RETRY_MS, MG_TIMER_REPEAT | MG_TIMER_RUN_NOW, retry_timer_handler, &mgr);

    mg_timer_add(&mgr, PING_MS, MG_TIMER_REPEAT | MG_TIMER_RUN_NOW, ping_timer_handler, &mgr);

    /*----------------------------------------------------------------------------------------------------------------*/

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    while(s_signo == 0)
    {
        mg_mgr_poll(&mgr, (int) POLL_MS);
    }

    /*----------------------------------------------------------------------------------------------------------------*/

    mg_mgr_free(&mgr);

    /*----------------------------------------------------------------------------------------------------------------*/

    MG_INFO(("Bye."));

    return 0;
}

/*--------------------------------------------------------------------------------------------------------------------*/
