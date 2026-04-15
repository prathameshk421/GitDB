class GitDBError(Exception):
    """Base error for GitDB."""


class ConfigError(GitDBError):
    pass


class SnapshotRowLimitError(GitDBError):
    pass


class CheckoutSchemaError(GitDBError):
    pass


class CheckoutDataError(GitDBError):
    pass

