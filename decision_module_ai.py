from typing import Dict

from threading import Thread

from events.event import *
from events.kafka_event import *

from trend_data.trend_data import TrendData
from scale_data.scale_data import ScaleData

from microservice.microservice import Microservice


class DecisionModuleAI(Microservice):
    '''
    Класс отвечающий за представление DM AI
    Его задача -- анализировать полученную информацию о тренде от DM Manager'a и возвращать количество реплик скалируемого приложения
    '''

    def __init__(self, event_queue: Queue, writers: Dict[str, KafkaEventWriter]):
        '''
        Инициализация класса
        '''

        return super().__init__(event_queue, writers)

    def handle_event(self, event: Event):
        '''
        Обработка ивентов, здесь находится основная логика микросервиса
        '''
        target_function = None

        match event.type:
            case EventType.TrendData:
                target_function = self.handle_event_trend_data
            case _:
                pass
        
        if target_function is not None:
            Thread(target=target_function, args=(event.data,)).start()

    def handle_event_trend_data(self, trend_data: TrendData):
        # send TrendAnalyseResult to DM Manager
        result = self.analyse_trend_data(trend_data)
        self.writers['dmm'].send_event(Event(EventType.TrendAnalyseResult, result))

    def analyse_trend_data(self, _trend_data: TrendData):
        return ScaleData(replica_count=2)
