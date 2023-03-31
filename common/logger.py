import os
import syslog

from datetime import datetime


class Logger(object):
    def __init__(self, name, context, plain):
        self.pid = os.getpid()
        self.name = name
        self.context = context
        self.plain = plain

        self.colors = dict(
            RED='\033[31m',
            GREEN='\033[32m',
            BLUE='\033[34m',
            YELLOW='\033[33m',
            MAGENTA='\033[35m',
            CYAN='\033[36m',
            GRAY='\033[37m',
            WHITE='\033[97m',
            BLACK='\033[30m',
            UNDERLINE='\033[4m',
            RESET='\033[0m'
        )

    def color(self):
        return {
            'search': self.colors['MAGENTA'],
            'crawler': self.colors['CYAN'],
            'pushshift': self.colors['GREEN'],
            'praw': self.colors['BLUE'],
            'main': self.colors['YELLOW']
        }[self.name]

    def text(self, colored=True):
        time = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
        pid = f'{self.pid}'
        name = f'{self.name:^9}'
        context = f'{self.context}'

        if colored:
            time = f'{self.colors["GRAY"]}{time}{self.colors["RESET"]}'
            pid = f'{self.colors["GRAY"]}{pid}{self.colors["RESET"]}'
            name = f'{self.color()}{name}{self.colors["RESET"]}'
            context = f'{self.colors["UNDERLINE"]}{context}{self.colors["RESET"]}'

        return f'{time} | {pid} | {name} | {context} '

    def log(self, text):
        if self.plain:
            print(f'{self.color()}{text}{self.colors["RESET"]}')
            syslog.syslog(f'{text}'.replace('\n', ''))
        else:
            print(f'{self.text(colored=True)}{text}')
            syslog.syslog(f'{self.text(colored=False)}{text}'.replace('\n', ''))
