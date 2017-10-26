import logging

import click
import six

from fedmsg import api
from . import cli


@cli.command()
@click.option('--topic', default=None, multiple=True, type=six.text_type,
              help='The topic to subscribe to; defaults to all topics')
@click.argument('endpoints', nargs=-1)
def subscribe(endpoints, topic=None):
    """Subscribe to a ZeroMQ publishing socket."""
    logging.basicConfig(level='INFO')
    messages = api.subscribe(endpoints, topic)
    for message in messages:
        print(message)
