import click

from por_que.exceptions import PorQueError


class InvalidValueError(PorQueError, click.UsageError, ValueError):
    pass
