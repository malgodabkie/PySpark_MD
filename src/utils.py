
import argparse
import logging
from logging.handlers import RotatingFileHandler

def parse_arguments() -> argparse.Namespace:
    """
    Function to parse parameters to run the application.
    
    Args:
        None
 
    Returns:
        args: Arguments specified by the User in the command line (or the default values if none provided).
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--filter_list', type=str, nargs='*', help='Countries to filter', default=['Netherlands', 'United Kingdom'])
    parser.add_argument('--clients', type=str, nargs='*', help='Path to the clients file', default='dataset_one.csv')
    parser.add_argument('--transactions', type=str, nargs='*', help='Path to the clients file', default='dataset_two.csv')
    args = parser.parse_args()
    return args

def create_rotating_log(path: str, size=2000):
    """
    Function to create a rotating log.
    
    Args:
        path (string): destination path where the logs should be stored
        size (integer): specifies maximum size of the log file
 
    Returns:
        df_selected (DataFrame): The new DataFrame object with only the selected columns included.
    """
    logger = logging.getLogger("Codac Rotating Logger")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(path, maxBytes=size, backupCount=4)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
