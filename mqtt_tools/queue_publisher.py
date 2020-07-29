#!/usr/bin/env python
"""MQTT client producing data."""

import collections
import threading
import warnings

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
        self._t = threading.Thread()

    def run(self, mqtthost):
        """Connect the client and start the queue thread."""
        self.connect(mqtthost)
        self.loop_start()
        self._start_q()

    def stop(self):
        """Stop the client loop.

        Does not disconnect the client normally to allow last will and testament to be
        used.
        """
        self._end_q()
        self.loop_stop()

    @property
    def q_size(self):
        """Get current length of deque."""
        return len(self._q)

    def _start_q(self):
        """Start queue and mqtt client threads.

        The MQTT client publishes data to a topic from its own queue.

        topic : str
            MQTT topic to publish to.
        """
        if self._t.is_alive() is False:
            self._q = collections.deque()
            self._t = threading.Thread(target=self._queue_publisher)
            self._t.start()
        else:
            warnings.warn(
                "A queue is already running. End that queue first or instantiate "
                + "a new queue publisher client."
            )

    def _end_q(self):
        """End a thread that publishes data from a queue."""
        if self._t.is_alive() is True:
            # send the queue thread a stop command
            self._q.appendleft(["stop", "", False])
            # join thread
            self._t.join()

    def append_payload(self, topic, payload, retain=False):
        """Append a payload to a queue.

        payload : str
            Message to be added to deque.
        topic : str
            Topic to publish to.
        retain : bool
            Flag whether or not the message should be retained.
        """
        self._q.append([payload, topic, retain])

    def _queue_publisher(self):
        """Publish elements in the queue.

        q : deque
            Deque to publish from.
        topic : str
            MQTT topic to publish to.
        """
        while True:
            if len(self._q) > 0:
                payload, topic, retain = self._q.popleft()
                if payload == "stop":
                    break
                # publish paylod with blocking wait for completion
                self.publish(topic, payload, 2, retain).wait_for_publish()

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object.

        Make sure everything gets cleaned up properly.
        """
        self.stop()


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
        qp.run(args.mqtthost)
        qp.loop_forever()
