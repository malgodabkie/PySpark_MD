from pyspark.sql import DataFrame
from logging import Logger

def select_columns_from_file(df: DataFrame, columns: list, logger: Logger) -> DataFrame:
    """
    Function to select specific list of columns from a DataFrame.
    
    Args:
        df (DataFrame): The DataFrame object from which you want to select columns.
        columns (list): The list object with the column names (str) to be selected.
 
    Returns:
        df_selected (DataFrame): The new DataFrame object with only the selected columns included.
    """
    logger.info(f'Selecting columns: {columns} from DataFrame')
    df_selected = df.select(columns)
    return df_selected

def filter_data_from_file(df: DataFrame, column_name_to_filter: str, filter_list: list, logger: Logger) -> DataFrame:
    """
    Function to filter data from a DataFrame based on column values.
    
    Args:
        df (DataFrame): The DataFrame object from which you want to select rows.
        column_name_to_filter (str): A name of the column that should be used for filtering.
        filter_list (list): 
 
    Returns:
        df_filtered (DataFrame): The new DataFrame object with only the rows that match the condition included.
    """
    logger.info(f'Filtering DataFrame on columns: {column_name_to_filter}')
    df_filtered = df.filter(df[column_name_to_filter].isin(filter_list))
    return df_filtered

def rename_columns_in_file(df: DataFrame, columns_to_rename: dict, logger: Logger) -> DataFrame:
    """
    Function to rename columns in a DataFrame based on the provided values.
    
    Args:
        df (DataFrame): The DataFrame object in which you want to rename the columns.
        columns_to_rename (dict): A dictionary storing current and expected column names. 
 
    Returns:
        df_renamed (DataFrame): The new DataFrame object with the columns renamed.
    """
    logger.info(f'Renaming columns: {columns_to_rename}')
    df_renamed = df
    for before, after in columns_to_rename.items():
        df_renamed = df_renamed.withColumnRenamed(before, after)
    return df_renamed

def join_tables(df1: DataFrame, df2: DataFrame, column_name: str, type: str, logger: Logger) -> DataFrame:
    """
    Function to join two DataFrames based on one common column.
    
    Args:
        df1 (DataFrame): The first DataFrame object used in the join (left).
        df1 (DataFrame): The second DataFrame object used in the join (right).
        column_name (str): The common column name that is used in the join.
        type (str): The type of the join to be used.
 
    Returns:
        df_joined (DataFrame): The new DataFrame object with the columns renamed.
    """
    logger.info(f'Joining DataFrames on column: {column_name}, type of join {type}')
    df_joined = df1.join(df2, [column_name], type)
    return df_joined
    