import logging
import yaml

# Configure root logger
def setup_logging() -> None:
    """ 
    Setup logging configuration for the app
    """
    config = {
        'version': 1,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
                'level': 'INFO'
            },
            'file': {
                'class': 'logging.FileHandler',
                'formatter': 'standard',
                'level': 'DEBUG',
                'filename': 'app.log'
            }
        },
        'root': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
        }
    }

    # Apply the configuration
    logging.config.dictConfig(config)