import os
from logging import Logger
from pyspark.sql import SparkSession

def find_files(path: str, file_name1: str, file_name2: str, logger: Logger):
    """
    Function to check if all the files required for the application to run correctly are available in the input folder.
    
    Args:
        path (string): The path to the folder where the input files should be stored.
        file_name1 (string): Name of the first file that needs to be provided.
        file_name2 (string): Name of the second file that needs to be provided.
        logger (Logger object): The Logger instance that allows to record status of the function execution.
 
    Returns:
        None if no exceptions were raised.
    """
    logger.info('Checking for files in the specified folder')
    files_in_folder = os.listdir(path)
    data_files_found = sorted(list(filter(lambda file: file.endswith('csv'), files_in_folder)))
    if file_name1 not in data_files_found:
        logger.error('Clients file not found, stopping the application')
        raise FileNotFoundError('Clients file not found')
    elif file_name2 not in data_files_found:
        logger.error('Transactions file not found, stopping the application')
        raise FileNotFoundError('Transactions file not found')
    else:
        logger.info('Found 2 data files, starting processing the files')

def read_source_file(spark: SparkSession, directory: str, file_name_to_load: str,
                        logger: Logger, schema):
    """
    Function to load the input files for the application to process.
    
    Args:
        spark (SparkSession): SparkSession object to read the source files.
        directory (string): Path to folder where the source files to read are stored.
        file_name_to_load (string): Name of the file that needs to be loaded.
        logger (Logger object): The Logger instance that allows to record status of the function execution.
        schema (DataFrame.schema): schema that is used to read the source file
 
    Returns:
        df (DataFrame): DataFrame object with the data loaded from the source file.
    """
    file_load_path = os.path.join(directory, file_name_to_load)
    file_load_path = file_load_path.replace("file:", "")
    logger.info(f"Loading data from the file path: {file_load_path}")

    df = spark.read.option("header", True).schema(schema).csv(file_load_path)
    logger.info('Dataframe loaded')
    return df


