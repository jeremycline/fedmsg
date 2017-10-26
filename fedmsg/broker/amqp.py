# This file is part of fedmsg.
# Copyright (C) 2017 Red Hat, Inc.

import logging
import json

import pika

from .base import Publisher

_log = logging.getLogger(__name__)


SUBMISSION_VERSION = b'FEDSUBMIT1'


class AmqpPublisher(Publisher):

    def __init__(self, amqp_url='amqp://guest:guest@localhost:5672/'):
        super(AmqpPublisher, self).__init__()
        _log.info('Starting AMQP connection to %s', amqp_url.split('@')[1])
        self.parameters = pika.URLParameters(amqp_url)

    def publish(self, topic, headers, body):
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.basic_publish(
            '',
            topic.encode('utf-8'),
            json.dumps(body).encode('utf-8'),
            pika.BasicProperties(
                content_type='application/json',
                content_encoding='utf-8',
                headers=headers,
                delivery_mode=1
            )
        )
        self.channel.close()
        self.connection.close()
