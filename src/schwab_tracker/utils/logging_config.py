import logging
from typing import Dict
from pathlib import Path

def setup_logging(config: Dict) -> None:
    """
    Set up logging configuration for the application.
    
    Args:
        config: Dictionary containing logging configuration settings
            Expected format:
            {
                'logging': {
                    'level': 'INFO',
                    'file': 'schwab_api.log',
                    'format': '%(message)s'
                }
            }
    """
    # Create logs directory if it doesn't exist
    log_file = Path(config['logging']['file'])
    log_file.parent.mkdir(exist_ok=True)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config['logging']['level']),
        format=config['logging']['format'],
        handlers=[
            logging.FileHandler(str(log_file)),
            logging.StreamHandler()  # Also output to console
        ]
    )

    # Create logger for the application
    logger = logging.getLogger('schwab_tracker')
    logger.setLevel(getattr(logging, config['logging']['level']))

    # Add more detailed formatting for debug level
    if config['logging']['level'] == 'DEBUG':
        debug_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        for handler in logger.handlers:
            handler.setFormatter(debug_formatter)

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.
    
    Args:
        name: Name of the module requesting the logger
        
    Returns:
        Logger instance configured for the module
    """
    return logging.getLogger(f'schwab_tracker.{name}')