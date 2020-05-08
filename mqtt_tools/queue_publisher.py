#!/usr/bin/env python
"""MQTT client producing data."""

import collections
import threading
import warnings

import paho.mqtt.client as mqtt

MQTTHOST = ""


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
        self._topic = None

    @property
    def topic(self):
        """Get topic attribute."""
        return self._topic

    @property
    def q_size(self):
        """Get current length of deque."""
        return len(self._q)

    def start_q(self, topic):
        """Start queue and mqtt client threads.

        The MQTT client publishes data to a topic from its own queue.

        topic : str
            MQTT topic to publish to.
        """
        if self._topic is None:
            self._topic = topic
            self.loop_start()  # start MQTT client thread
            self._q = collections.deque()
            self._t = threading.Thread(target=self._queue_publisher)
            self._t.start()
        else:
            warnings.warn(
                f"A queue for '{self._topic}' is already running. End that queue first or instantiate a new queue publisher client."
            )

    def end_q(self):
        """End a thread that publishes data to a topic from its own queue."""
        self._q.appendleft("stop")  # send the queue thread a stop command
        self._t.join()  # join thread
        self.loop_stop()
        self._topic = None  # forget thread and queue

    def append_payload(self, payload):
        """Append a payload to a queue.

        payload : str
            Message to be added to deque.
        """
        self._q.append(payload)

    def _queue_publisher(self):
        """Publish elements in the queue.

        q : deque
            Deque to publish from.
        topic : str
            MQTT topic to publish to.
        """
        while True:
            if len(self._q) > 0:
                payload = self._q.popleft()
                if payload == "stop":
                    break
                # publish paylod with blocking wait for completion
                self.publish(self._topic, payload, qos=2).wait_for_publish()

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object.

        Make sure everything gets cleaned up properly.
        """
        self.end_q()
        self.disconnect()
