class Prompt(object):
    def __init__(self, text, default=None, auto=None):
        self.result = None

        while self.result is None:
            prompt = {None: '[y/n]', True: '[Y/n]', False: '[y/N]'}[default]
            choice = input(f'{text} {prompt} ') if not auto else default

            if not choice and default is not None:
                self.result = default
            elif self.boolean(choice) is not None:
                self.result = self.boolean(choice)

    def yes(self):
        return self.result

    def no(self):
        return not self.result

    @staticmethod
    def boolean(obj):
        values = {'true': True, 't': True, 'yes': True, 'y': True, '1': True, 'false': False, 'f': False, 'no': False, 'n': False, '0': False}
        value = str(obj).lower()
        return None if value not in values else values[value]
