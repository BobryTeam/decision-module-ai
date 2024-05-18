import threading
from threading import Thread

from events.event import *
from events.kafka_event import *

from trend_data.trend_data import TrendData
from scale_data.scale_data import ScaleData



class DecisionModuleAI:
    '''
    Класс отвечающий за представление DM AI
    Его задача -- анализировать полученную информацию о тренде от DM Manager'a и возвращать количество реплик скалируемого приложения
    '''

    def __init__(self, dm_manager_event_writer: KafkaEventWriter, event_queue: Queue):
        '''
        Инициализация класса
        '''

        self.event_queue = event_queue

        self.dm_manager_event_writer = dm_manager_event_writer

        self.running = threading.Event()
        self.running.set()

        self.running_thread = Thread(target=self.run)
        self.running_thread.start()

    def run(self):
        '''
        Принятие событий из очереди 
        '''
        while self.running.is_set() or not self.event_queue.empty():
            if self.event_queue.empty(): continue
            event_thread = Thread(target=self.handle_event, args=((self.event_queue.get()),))
            event_thread.start()

    def handle_event(self, event: Event):
        '''
        Обработка ивентов, здесь находится основная логика микросервиса
        '''
        match event.type:
            case EventType.TrendData:
                self.handle_event_trend_data(event.data)
            case _:
                pass

    def handle_event_trend_data(self, trend_data: TrendData):
        # send TrendAnalyseResult to DM Manager
        result = self.analyse_trend_data(trend_data)
        self.dm_manager_event_writer.send_event(Event(EventType.TrendAnalyseResult, result))
    
    def analyse_trend_data(self, _trend_data: TrendData):
        return ScaleData(replica_count=2)

    def stop(self):
        self.running.clear()
