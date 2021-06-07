#!/usr/bin/env python
"""MQTT client producing data."""

import queue
import threading

import paho.mqtt.client as mqtt


class MQTTQueuePublisher(mqtt.Client):
    """MQTT client that publishes data to a topic from its own queue.

    Publishing to a topic can take a significant amount of time. If data is produced
    too quickly, appending it to a queue is faster than publishing, allowing the
    program to continue without blocking. Messages can be published from the queue
    concurrently without blocking the main program producing data.
    """

    def __init__(self):
        """Construct MQTT client, inheriting from mqtt.Client.

        Callback and connect methods are not automatically run here. They should be
        called in the same way as for the base mqtt.Client.
        """
        super().__init__()
        self._start_q()

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object.

        Make sure everything gets cleaned up properly.
        """
        self.stop()

    def stop(self):
        """Stop the queue publisher."""
        # wait for all messages to be published
        while True:
            if self._q.empty() is True:
                break
        self.loop_stop()
        self.disconnect()

    @property
    def q_size(self):
        """Get current length of queue."""
        return self._q.qsize()

    def _start_q(self):
        """Start queue and mqtt client threads.

        The MQTT client publishes data to a topic from its own queue.

        topic : str
            MQTT topic to publish to.
        """
        self._q = queue.Queue()
        threading.Thread(target=self._queue_publisher, daemon=True).start()

    def _queue_publisher(self):
        """Publish elements in the queue.

        q : queue.Queue
            Queue to publish from.
        topic : str
            MQTT topic to publish to.
        """
        while True:
            topic, payload, retain = self._q.get()
            # publish paylod with blocking wait for completion
            self.publish(topic, payload, 2, retain).wait_for_publish()
            self._q.task_done()

    def append_payload(self, topic, payload, retain=False):
        """Append a payload to a queue.

        payload : str
            Message to be added to queue.
        topic : str
            Topic to publish to.
        retain : bool
            Flag whether or not the message should be retained.
        """
        self._q.put_nowait([topic, payload, retain])


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mqtthost",
        default="127.0.0.1",
        help="IP address or hostname of MQTT broker.",
    )
    args = parser.parse_args()

    with MQTTQueuePublisher as qp:
        # connect MQTT client to broker
        qp.connect(args.mqtthost)
        qp.loop_forever()
