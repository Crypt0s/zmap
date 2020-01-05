/*
 * ZMap Copyright 2013 Regents of the University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <inttypes.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "../../lib/logger.h"
#include "../../lib/xalloc.h"
#include "../../lib/librdkafka.h"

#include "output_modules.h"

#define UNUSED __attribute__((unused))

#define BUFFER_SIZE 1000

static char **buffer;
static int buffer_fill = 0;
static char *queue_name = NULL;

// Kafka Globals.  Wish for namespaces /shrug
rd_kafka_t *rk;         /* Producer instance handle */
rd_kafka_conf_t *conf;  /* Temporary configuration object */
char *topic;      /* Argument: topic to produce to */


static void dr_msg_cb (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
    else
        fprintf(stderr,
                "%% Message delivered (%zd bytes, "
                "partition %"PRId32")\n",
                rkmessage->len, rkmessage->partition);
    /* The rkmessage is destroyed automatically by librdkafka */
}


int kafkamodule_init(struct state_conf *conf, UNUSED char **fields,
			UNUSED int fieldlens)
{

    char errstr[512];       /* librdkafka API error reporting buffer */
    char buf[512];          /* Message value temporary buffer */
    const char *brokers;    /* Argument: broker list */

    char * connect_string = NULL;
    if (conf->output_args) {
        connect_string = conf->output_args;
        printf("connect_string: %s", connect_string);
    }else{
        connect_string = "localhost:9092/yeet";
    }

    char * servername = (char *)malloc(strlen(connect_string));
    char * topicname = (char *)malloc(strlen(connect_string));
    //uint32_t port = 0;

    if (sscanf(connect_string, "%[^/]/%s", servername, topicname) != 2){
        // TODO: Handle this error better.
        printf("output module argument was unable to be parsed - format is wrong");
        printf ("server: %s\ntopic: %s\n",servername,topicname);
        exit(1);
    }

    topic = topicname;

    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", servername, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        exit(2);
    }
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr,
                "%% Failed to create new producer: %s\n", errstr);
        exit(3);
    }
	return EXIT_SUCCESS;
}

static int kafkamodule_flush(void)
{
    /* Wait for final messages to be delivered or fail.
     * rd_kafka_flush() is an abstraction over rd_kafka_poll() which
     * waits for all messages to be delivered. */
    fprintf(stderr, "%% Flushing final messages..\n");
    rd_kafka_flush(rk, 10*1000 /* wait for max 10 seconds */);

    /* If the output queue is still not empty there is an issue
     * with producing messages to the clusters. */
    if (rd_kafka_outq_len(rk) > 0)
        fprintf(stderr, "%% %d message(s) were not delivered\n",
                rd_kafka_outq_len(rk));
	for (int i = 0; i < buffer_fill; i++) {
		free(buffer[i]);
	}
	buffer_fill = 0;
	return EXIT_SUCCESS;
}

#define INT_STR_LEN 20 // len(9223372036854775807) == 19

static size_t guess_csv_string_length(fieldset_t *fs)
{
	size_t len = 0;
	for (int i = 0; i < fs->len; i++) {
		field_t *f = &(fs->fields[i]);
		if (f->type == FS_STRING) {
			len += strlen(f->value.ptr);
			len += 2; // potential quotes
		} else if (f->type == FS_UINT64) {
			len += INT_STR_LEN;
		} else if (f->type == FS_BOOL) {
			len +=
			    INT_STR_LEN; // 0 or 1 PRIi32 is used to print ...
		} else if (f->type == FS_BINARY) {
			len += 2 * f->len;
		} else if (f->type == FS_NULL) {
			// do nothing
		} else {
			log_fatal("csv",
				  "received unknown output type "
				  "(not str, binary, null, or uint64_t)");
		}
	}
	// estimated length + number of commas
	return len + (size_t)len + 256;
}

static void hex_encode_str(char *f, unsigned char *readbuf, size_t len)
{
	char *temp = f;
	for (size_t i = 0; i < len; i++) {
		sprintf(temp, "%02x", readbuf[i]);
		temp += (size_t)2 * sizeof(char);
	}
}

void make_csv_string(fieldset_t *fs, char *out, size_t len)
{
	memset(out, 0, len);
	for (int i = 0; i < fs->len; i++) {
		char *temp = out + (size_t)strlen(out);
		field_t *f = &(fs->fields[i]);
		char *dataloc = temp;
		if (i) { // only add comma if not first element
			sprintf(temp, ",");
			dataloc += (size_t)1;
		}
		if (f->type == FS_STRING) {
			if (strlen(dataloc) + strlen((char *)f->value.ptr) >=
			    len) {
				log_fatal("redis-csv",
					  "out of memory---will overflow");
			}
			if (strchr((char *)f->value.ptr, ',')) {
				sprintf(dataloc, "\"%s\"",
					(char *)f->value.ptr);
			} else {
				sprintf(dataloc, "%s", (char *)f->value.ptr);
			}
		} else if (f->type == FS_UINT64) {
			if (strlen(dataloc) + INT_STR_LEN >= len) {
				log_fatal("redis-csv",
					  "out of memory---will overflow");
			}
			sprintf(dataloc, "%" PRIu64, (uint64_t)f->value.num);
		} else if (f->type == FS_BOOL) {
			if (strlen(dataloc) + INT_STR_LEN >= len) {
				log_fatal("redis-csv",
					  "out of memory---will overflow");
			}
			sprintf(dataloc, "%" PRIi32, (int)f->value.num);
		} else if (f->type == FS_BINARY) {
			if (strlen(dataloc) + 2 * f->len >= len) {
				log_fatal("redis-csv",
					  "out of memory---will overflow");
			}
			hex_encode_str(dataloc, (unsigned char *)f->value.ptr,
				       f->len);
		} else if (f->type == FS_NULL) {
			// do nothing
		} else {
			log_fatal("redis-csv", "received unknown output type");
		}
	}
}

int kafkamodule_process(fieldset_t *fs)
{
    rd_kafka_resp_err_t err;

    size_t reqd_space = guess_csv_string_length(fs);
    char *buf = xmalloc(reqd_space);
    make_csv_string(fs, buf, reqd_space);


    retry:
    err = rd_kafka_producev(
            /* Producer handle */
            rk,
            /* Topic name */
            RD_KAFKA_V_TOPIC(topic),
            /* Make a copy of the payload. */
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            /* Message value and length */
            RD_KAFKA_V_VALUE(buf, strlen(buf)),
            /* Per-Message opaque, provided in
             * delivery report callback as
             * msg_opaque. */
            RD_KAFKA_V_OPAQUE(NULL),
            /* End sentinel */
            RD_KAFKA_V_END);

    if (err) {
        /*
         * Failed to *enqueue* message for producing.
         */
        fprintf(stderr,
                "%% Failed to produce to topic %s: %s\n",
                topic, rd_kafka_err2str(err));

        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            /* If the internal queue is full, wait for
             * messages to be delivered and then retry.
             * The internal queue represents both
             * messages to be sent and messages that have
             * been sent or failed, awaiting their
             * delivery report callback to be called.
             *
             * The internal queue is limited by the
             * configuration property
             * queue.buffering.max.messages */
            rd_kafka_poll(rk, 1000/*block for max 1000ms*/);
            goto retry;
        }
    } else {
        fprintf(stderr, "%% Enqueued message (%zd bytes) "
                        "for topic %s\n",
                strlen(buf), topic);
    }


    /* A producer application should continually serve
     * the delivery report queue by calling rd_kafka_poll()
     * at frequent intervals.
     * Either put the poll call in your main loop, or in a
     * dedicated thread, or call it after every
     * rd_kafka_produce() call.
     * Just make sure that rd_kafka_poll() is still called
     * during periods where you are not producing any messages
     * to make sure previously produced messages have their
     * delivery report callback served (and any other callbacks
     * you register). */
    rd_kafka_poll(rk, 0/*non-blocking*/);

	return EXIT_SUCCESS;
}

int kafkamodule_close(UNUSED struct state_conf *c,
			 UNUSED struct state_send *s,
			 UNUSED struct state_recv *r)
{

	if (kafkamodule_flush()) {
		return EXIT_FAILURE;
	}
	rd_kafka_destroy(rk);
	return EXIT_SUCCESS;
}

output_module_t module_kafka = {
    .name = "kafka",
    .init = &kafkamodule_init,
    .start = NULL,
    .update = NULL,
    .update_interval = 0,
    .close = &kafkamodule_close,
    .process_ip = &kafkamodule_process,
    .supports_dynamic_output = NO_DYNAMIC_SUPPORT,
    .helptext =
	"Outputs entries to Kafka using librdkafka. \n"
	"By default, the probe module does not filter out duplicates or limit to successful fields, \n"
	"but rather includes all received packets. Fields can be controlled by \n"
	"setting --output-fields. Filtering out failures and duplicate packets can \n"
	"be achieved by setting an --output-filter."};
