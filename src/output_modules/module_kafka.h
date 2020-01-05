/*
 * ZMap Copyright 2013 Regents of the University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at http://www.apache.org/licenses/LICENSE-2.0
 */

#include "output_modules.h"

int kafkamodule_init(struct state_conf *conf, char **fields, int fieldlens);
int kafkamodule_process(fieldset_t *fs);
int kafkamodule_close(struct state_conf *c, struct state_send *s, struct state_recv *r);

static void dr_msg_cb (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *opaque) {
    if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n",
                rd_kafka_err2str(rkmessage->err));
    else
        fprintf(stderr,
                "%% Message delivered (%zd bytes, "
                "partition %"PRId32")\n",
            rkmessage->len, rkmessage->partition);

    /* The rkmessage is destroyed automatically by librdkafka */
}