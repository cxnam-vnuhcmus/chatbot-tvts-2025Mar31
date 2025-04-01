
class PerfDto:
    def __init__(self, start_time=None, end_time=None):
        self.start_time = start_time
        self.end_time = end_time

    def __dict__(self):
        return dict(
            start_time = self.start_time,
            end_time = self.end_time,
        )