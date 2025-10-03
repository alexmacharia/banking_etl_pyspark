import logging

# Configure root logger
def setup_loging() -> None:
    """ 
    Setup logging configuration for the app
    """
    logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s - %(levelname)s - %(message)s',
        filename = 'app.log',
        filemode = 'a'
    )