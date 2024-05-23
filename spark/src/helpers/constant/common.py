class CommonConstant(object):
    TRUE: str = 'true'
    FALSE: str = 'false'
    ZERO: int = 0
    ONE: int = 1
    TWO: int = 2
    ENTER: str = '\n'
    CHARSET_UTF8: str = 'UTF-8'
    MONTHLY: int = 31
    WEEKLY: int = 7

    DEFAULT_START_HHmmss: str = '00:00:00'
    DEFAULT_END_HHmmss: str = '23:59:59'
    DEFAULT_NLS_DATE_FORMAT: str = 'YYYY-MM-DD HH24:MI:SS'
    DEFAULT_NLS_DATE_MS_FORMAT: str = 'YYYY-MM-DD HH24:MI:SS.MS'
    DEFAULT_INTERVAL_HOUR: str = '7'
    DEFAULT_UPDATE_DAYS: int = 3
    DEFAULT_ADJUST_DAYS: int = 91
    DEFAULT_DATE_FORMAT: str = '%Y-%m-%d'

    KST_START_HHmmss: str = '09:00:00'
    KST_END_HHmmss: str = '08:59:59'
    UTC: int = 0
    KST: int = 9

    ENV_DEFAULT: str = 'test'
    ENV_TEST: str = 'test'
    ENV_LOCAL: str = 'local'
    ENV_DEVELOP: str = 'develop'
    ENV_PRODUCTION: str = 'production'