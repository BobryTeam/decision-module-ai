from typing import Dict
from queue import Queue

from threading import Thread

from events import *
from trend_data import TrendData
from scale_data import ScaleData
from microservice import Microservice
from dm_ai_model import DecisionModuleModel

import pkg_resources



class DecisionModuleAI(Microservice):
    '''
    Класс отвечающий за представление DM AI
    Его задача -- анализировать полученную информацию о тренде от DM Manager'a и возвращать количество реплик скалируемого приложения
    '''

    def __init__(self, event_queue: Queue, writers: Dict[str, KafkaEventWriter]):
        '''
        Инициализация класса
        '''
        self.model = DecisionModuleModel(pkg_resources.resource_filename('dm_ai_model', 'model.joblib'))

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

    def analyse_trend_data(self, trend_data: TrendData):
        return ScaleData(self.model.analyze(trend_data))
