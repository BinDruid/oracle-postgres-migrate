from django.core.management.base import BaseCommand


class Logger:
    def __init__(self):
        self._logger = BaseCommand()

    def write_error(self, msg, *args, **kwargs):
        self._logger.stdout.write(self._logger.style.ERROR(msg), *args, **kwargs)

    def write_success(self, msg, *args, **kwargs):
        self._logger.stdout.write(self._logger.style.SUCCESS(msg), *args, **kwargs)

    def write_warning(self, msg, *args, **kwargs):
        self._logger.stdout.write(self._logger.style.WARNING(msg), *args, **kwargs)

    def progress(self, iteration, total, suffix=""):
        prefix = "Progress:"
        decimals = 1
        fill = "█"
        print_end = "\r"
        length = 25
        percent = ("{0:." + str(decimals) + "f}").format(
            100 * (iteration / float(total))
        )
        filled_length = int(length * iteration // total)
        bar = fill * filled_length + "-" * (length - filled_length)
        self.write_success(
            msg=(f"\r{prefix} |{bar}| {percent} % | {suffix}"), ending=print_end
        )
        if iteration == total:
            print()
