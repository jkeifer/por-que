def get_version() -> str:
    try:
        from . import __version__

        return __version__.__version__
    except ImportError:
        return 'development version'
