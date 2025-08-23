import logging
import logging.config
import os

from dotenv import load_dotenv
load_dotenv()

def setup_logging():
    """Configure logging once at application startup"""
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'detailed': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
            },
            'simple': {
                'format': '%(levelname)s - %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'detailed',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': 'INFO',
                'formatter': 'detailed',
                'filename': 'rfq_market_maker.log',
                'maxBytes': 10485760,  # 10MB
                'backupCount': 5
            }
        },
        'loggers': {
            '': {  # root logger
                'level': os.getenv('LOG_LEVEL', 'INFO'),
                'handlers': ['console', 'file']
            },
            'websocket_client': {
                'level': 'DEBUG' if os.getenv('DEBUG') else 'INFO',
                'propagate': True
            },
            'market_maker': {
                'level': 'DEBUG' if os.getenv('DEBUG') else 'INFO',
                'propagate': True
            },
            # Reduce noise from third-party libraries
            'asyncio': {
                'level': 'WARNING'
            },
            'websockets': {
                'level': 'WARNING'
            }
        }
    }
    
    logging.config.dictConfig(config)