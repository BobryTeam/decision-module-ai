from decision_module_ai import DecisionModuleAI

from events.kafka_event import *
from events.event import *

import os

# put the values in a k8s manifest
kafka_bootstrap_server = os.environ.get('KAFKA_BOOTSTRAP_SERVER')

if kafka_bootstrap_server is None:
    print(f'Environment variable KAFKA_BOOTSTRAP_SERVER is not set')
    exit(1)


event_queue = Queue()

# share the queue with a reader
reader = KafkaEventReader(
    KafkaConsumer(
        'dmai',
        bootstrap_servers=kafka_bootstrap_server,
        auto_offset_reset='latest',
    ),
    event_queue
)

# writers to other microservices
writers = {
    'dmm': KafkaEventWriter(
        KafkaProducer(
            bootstrap_servers=kafka_bootstrap_server
        ),
        'dmm'
    ),
}

# init the microservice
decision_module_ai = DecisionModuleAI(
    event_queue, writers
)

decision_module_ai.running_thread.join()