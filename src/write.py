import os
from logging import Logger
from pyspark.sql import DataFrame
import shutil

def save_app_output(final_output: DataFrame, temporary_folder: str, final_output_folder: str, log: Logger):
    """
    Function to save the final DataFrame
    
    Args:
        df (DataFrame): The DataFrame object from which you want to select columns.
        columns (list): The list object with the column names (str) to be selected.
 
    Returns:
        df_selected (DataFrame): The new DataFrame object with only the selected columns included.
    """
    log.info('Loading the output file')
    final_output.write.mode("overwrite").option("header", "true").csv(temporary_folder)

    part_file = [f for f in os.listdir(temporary_folder) if f.startswith("part-")][0]
    shutil.move(os.path.join(temporary_folder, part_file), final_output_folder)

    shutil.rmtree(temporary_folder)
    log.info('Output saved in the destination folder')