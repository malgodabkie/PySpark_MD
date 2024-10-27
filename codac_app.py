from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType 
from src.load import find_files, read_source_file
from src.transform import select_columns_from_file, filter_data_from_file, rename_columns_in_file, join_tables
from src.utils import parse_arguments, create_rotating_log
from src.write import save_app_output
import os

client_schema = StructType(fields=[StructField('id', IntegerType(), False),
                                StructField('first_name', StringType(), True),
                                StructField('last_name', StringType(), True),
                                StructField('email', StringType(), True),
                                StructField('country', StringType(), True)])

transaction_schema = StructType(fields=[StructField('id', IntegerType(), False),
                                StructField('btc_a', StringType(), True),
                                StructField('cc_t', StringType(), True),
                                StructField('cc_n', LongType(), True)])

spark = SparkSession.builder.appName("Codac").master("local[1]").getOrCreate()

current_dir = os.path.dirname(os.path.abspath(__file__))
log_location = "logs/Codac.log"
log_file_path = os.path.join(current_dir, log_location)
log = create_rotating_log(log_file_path)
log.info('Started a new run of the application')

clients_columns_to_select = ["id", "email", "country"]
transactions_columns_to_select = ["id", "btc_a", "cc_t"]
columns_to_rename = {'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'}

args = parse_arguments()
filter_list = args.filter_list
clients_input_file = args.clients
transations_input_file = args.transactions

input_location = 'data\\raw\\'
source_file_path = os.path.join(current_dir, input_location)


# Load data
find_files(source_file_path, clients_input_file, transations_input_file, log)

clients_df = read_source_file(spark, source_file_path, clients_input_file, log, client_schema)
transactions_df = read_source_file(spark, source_file_path, transations_input_file, log, transaction_schema)

# Select columns
clients_selected_df = select_columns_from_file(clients_df, clients_columns_to_select, log)
transactions_selected_df = select_columns_from_file(transactions_df, transactions_columns_to_select, log)

# Filter data
clients_filtered_df = filter_data_from_file(clients_selected_df, "country", filter_list, log)

# Join tables
joined_df = join_tables(clients_filtered_df, transactions_selected_df, 'id', 'inner', log)

# Rename columns
final_df = rename_columns_in_file(joined_df, columns_to_rename, log)

# Generate output
final_df.repartition(1)

temp_folder_name = "data\\output\\temp"
final_output_folder_name = "data\\output\\output.csv"
temp_dir = os.path.join(current_dir, temp_folder_name)
final_output_dir = os.path.join(current_dir, final_output_folder_name)

save_app_output(final_df, temp_dir, final_output_dir, log)

spark.stop()
