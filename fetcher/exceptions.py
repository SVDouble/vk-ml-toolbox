class FetcherError(Exception):
    pass


class NoTokenError(FetcherError):
    pass


class FileDamagedError(FetcherError):
    pass


class DamagedEntitiesFoundError(FetcherError):
    pass
