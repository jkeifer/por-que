import urllib.request


def check_url(url: str) -> None:
    if not url.startswith(('http:', 'https:')):
        raise ValueError("URL must start with 'http:' or 'https:'")


def get_length(url: str) -> int:
    check_url(url)

    # security rule S310 mitigated by check_url() call
    request = urllib.request.Request(  # noqa: S310
        url,
        headers={'Range': 'bytes=0-0'},
    )

    with urllib.request.urlopen(request) as response:  # noqa: S310
        content_range = response.headers.get('Content-Range', None)

        if content_range is None:
            raise RuntimeError(
                f'Unable to get Content-Range for url, cannot determine length: {url}',
            )

        return int(content_range.split('/')[-1])


def get_bytes(url: str, start: int, end: int) -> bytes:
    check_url(url)

    if start >= end:
        raise ValueError(
            f'Starting byte must be less than ending byte: {start} >= {end}',
        )

    # security rule S310 mitigated by check_url() call
    request = urllib.request.Request(  # noqa: S310
        url,
        headers={'Range': f'bytes={start}-{end - 1}'},
    )

    with urllib.request.urlopen(request) as response:  # noqa: S310
        return response.read()
