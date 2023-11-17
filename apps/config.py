from os import environ 

ADMIN_EMAIL = environ.get('ADMIN_EMAIL')
MAILGUN_API_KEY = environ.get('MAILGUN_API_KEY')
MAILGUN_API_URL = environ.get('MAILGUN_API_URL')
MAILGUN_FROM_EMAIL = environ.get('MAILGUN_FROM_EMAIL')
SESSION_TYPE = environ.get('SESSION_TYPE')
SECRET_KEY = environ.get('SECRET_KEY')
TEMPLATE_URL = environ.get('TEMPLATE_URL')
DEFAULT_CHARSET = environ.get('DEFAULT_CHARSET')