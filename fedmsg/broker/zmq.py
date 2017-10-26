# -*- coding: utf-8 -*-
#
# This file is part of fedmsg.
# Copyright (C) 2017 Red Hat, Inc.
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
"""ZeroMQ publisher for the fedmsg publisher service."""

import logging
import json

import zmq

from .base import Publisher

_log = logging.getLogger(__name__)


class ZeroMqPublisher(Publisher):
    """
    Publisher that sends messages via a ZeroMQ PUB socket.

    Args:
        context (zmq.Context): The ZeroMQ context to use when creating the
            pair socket. If one is not provided, the default instance is used.
        publish_endpoint (str): The endpoint to bind the publishing socket to in
            standard ZeroMQ format ('tcp://127.0.0.1:9940', 'ipc:///path/to/sock', etc).
    """

    publisher_version = b'FEDPUB1'

    def __init__(self, context=None, publish_endpoint='tcp://0.0.0.0:9940'):
        super(ZeroMqPublisher, self).__init__(context=context)

        context = context or zmq.Context.instance()
        self.publish_endpoint = publish_endpoint
        self.pub_socket = context.socket(zmq.PUB)
        _log.info('Binding to %s for ZeroMQ publication', publish_endpoint)
        self.pub_socket.bind(publish_endpoint)

    def publish(self, topic, headers, body):
        """
        Publish messages to a ZeroMQ PUB socket.

        Args:
            topic (six.text_type): The ZeroMQ topic this message was sent to as a unicode string.
                This may contain any unicode character and implementations must account for that.
            headers (dict): A set of message headers. Each key and value will be a unicode string.
            body (dict): The message body.
        """

        _log.info('Publishing message on "%s" to the ZeroMQ PUB socket "%s"',
                  topic, self.publish_endpoint)
        topic = topic.encode('utf-8')
        headers = json.dumps(headers).encode('utf-8')
        body = json.dumps(body).encode('utf-8')
        multipart_message = [topic, self.publisher_version, headers, body]
        self.pub_socket.send_multipart(multipart_message)
