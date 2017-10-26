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
"""
The fedmsg broker service.

The broker binds to a submission port and relays incoming messages to publishers
which are responsible for publishing the message to the world via ZeroMQ, AMQP,
carrier pigeon, etc.
"""
from __future__ import absolute_import

import logging
import json
import os

import zmq

from .publishers import ZeroMqPublisher


SUBMISSION_V1 = b'FSUBMIT1'
ERROR_MALFORMED = b'ERROR_MALFORMED'

_log = logging.getLogger(__name__)


def broker_service(submission_endpoint, publisher_configs, context=None):
    """
    The fedmsg broker service.

    This service binds to the given submission socket using a ZeroMQ Reply socket
    where clients can send publishing requests using a ZeroMQ Request socket. When
    all publishers have announced success or failure, the client is informed.

    The fedmsg submission message is a multi-part message that builds on top of
    the ZeroMQ `REQREP`_ RFC. The message should be made up of 4 data frames:

        * Frame 0: The fedmsg submission protocol version string. This is an ASCII-encoded
          string that indicates the protocol version being used. There is currently only
          one version, "FSUBMIT1".
        * Frame 1: The message topic as a UTF-8-encoded string.
        * Frame 2: A set of key-value pairs to use as the message headers. This should be
          a JSON-serialized object encoded with UTF-8.
        * Frame 3: The message body as a JSON-serialized object encoded with UTF-8.


    .. _REQREP: https://rfc.zeromq.org/spec:28/REQREP

    Args:
        submission_socket (str): A ZeroMQ socket to bind to for message submission.
        publisher_configs (list): A list of dictionaries containing publisher configurations.
        context (zmq.Context): The ZeroMQ context to use when creating sockets. If one is
            not provided, one will be created.
    """
    if context is None:
        context = zmq.Context.instance()

    if submission_endpoint.startswith('ipc://'):
        socket_dir = os.path.dirname(submission_endpoint.split('ipc://')[1])

        # TODO err handling
        if not os.path.exists(socket_dir):
            _log.info('%s does not exist, creating it for the submission endpoint', socket_dir)
            os.makedirs(socket_dir, mode=0o775)

    _log.info('Binding to %s as the fedmsg publisher submission socket', submission_endpoint)
    submission_socket = context.socket(zmq.REP)
    submission_socket.bind(submission_endpoint)

    # Create and start the publishers and start the PAIR sockets for communication to each thread
    publishers = [(ZeroMqPublisher(**publisher_configs['zmq']), context.socket(zmq.PAIR))]
    for pub, sock in publishers:
        pub.start()
        sock.connect(pub.pair_address)

    while True:
        multipart_message = submission_socket.recv_multipart()
        _log.debug('Received message submission: %r', multipart_message)
        if len(multipart_message) != 4 or multipart_message[0] != SUBMISSION_V1:
            # Later we can switch on the submission version to allow for graceful
            # handling of protocol changes.
            _log.error('The submitted message, "%r", is malformed', multipart_message)
            submission_socket.send_multipart([SUBMISSION_V1, ERROR_MALFORMED])
        else:
            for _, publish_pair in publishers:
                publish_pair.send_multipart(multipart_message[1:])
                _log.info('Sent message to %s', _)

            # Block until everyone is done
            results = []
            for publisher, publish_pair in publishers:
                result = publish_pair.recv()
                results.append(json.loads(result.decode('utf-8')))
                _log.info('%s finished publishing and reported %s', publisher, result)

            submission_socket.send_multipart(
                [SUBMISSION_V1, json.dumps(results).encode('utf-8')])
