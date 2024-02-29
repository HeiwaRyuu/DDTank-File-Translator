import re
import time
import numpy as np
import pandas as pd
import datetime as dt
import translators as ts
from googletrans import Translator, LANGUAGES
from concurrent.futures import ProcessPoolExecutor

STANDARD_SLEEP_TIME = 1
STANDARD_NUMBER_OF_PARTITIONS = 20
translator = Translator()
manually_translate = []

def fetch_schema_and_table_names(row):
    row = row[0:row.find('(')]
    row_lst = row.split('.')
    db_name = row_lst[0][row_lst[0].find('[')+1:row_lst[0].find(']')]
    schema_name = row_lst[1][row_lst[1].find('[')+1:row_lst[1].find(']')]
    return db_name, schema_name

def fetch_column_names(row):
    ## FETCHING DATAFRAME COLUMNS
    columns = row[row.find('('):row.find(')')]
    # Characters to remove
    chars_to_remove = '()[] '
    # Create a translation table: each character to be removed maps to None
    trans_table = str.maketrans('', '', chars_to_remove)
    columns = columns.translate(trans_table).split(',')
    return columns

def fetch_values(row):
    values = row[row.find("VALUES")+8:-2]
    pattern = pattern = r", (?=(?:[^']*'[^']*')*[^']*$)"
    values = re.split(pattern, values)
    return values
    
def permeate_values_on_df(columns, raw_data):
    # Assuming 'data' is a list of strings
    df_raw = pd.DataFrame(raw_data, columns=['raw_data'])
    # Example parsing operation: extracting part after a delimiter
    df_raw['raw_data'] = df_raw['raw_data'].apply(fetch_values)
    df_new = pd.DataFrame(columns=columns)
    df_new[columns] = pd.DataFrame(df_raw['raw_data'].to_list())
    df_new = df_new.dropna() ## REMOVING ANY COLUMN THAT HAS EMPTY (None) VALUES
    return df_new

def translate(raw_untranslated_text, delay=STANDARD_SLEEP_TIME):
    translated_text = ''
    if len(raw_untranslated_text[0])>0:
        translated_text_en = ts.translate_text(query_text=raw_untranslated_text[0], translator = 'bing', from_language = 'auto', to_language = 'en')
        time.sleep(delay)  # Wait for a specified delay (in seconds) between requests
        translated_text = ts.translate_text(query_text=translated_text_en, translator = 'bing', from_language = 'en', to_language = 'pt')
        time.sleep(delay)  # Wait for a specified delay (in seconds) between requests
        translated_text = ts.translate_text(query_text=translated_text_en, translator = 'bing', from_language = 'auto', to_language = 'pt')
        chars_to_remove = '\'\"'
        translated_text = translated_text.maketrans('', '', chars_to_remove)
    return f"N'{translated_text}'"

# Function to translate text with delay to avoid rate limits
def translate_text_with_delay(text):
    try:
        raw_untranslated_text = re.findall(r'\'(.*?)\'', text)
    except Exception as e:
        print(f"Exception when parsing regex on untranslated text {text}: Exception: {e}")
        raw_untranslated_text = ['']
    max_attempts = 5
    attempts = 0
    while(attempts <= max_attempts):
        try:
            print(f"Translating text: {raw_untranslated_text}...")
            original_format_translated_text = translate(raw_untranslated_text)
            return original_format_translated_text
        except Exception as e:
            attempts += 1
            print(f"Exception while translating code: Exception: {e}... Trying again... Attempt number {attempts}")
            time.sleep(STANDARD_SLEEP_TIME*20)
    if attempts > max_attempts:
        print(f"Max attempts on the row with the following text: {text}")
        manually_translate.append(text)
    
    return text

# RECONSTRUCTING SQL TRANSLATED FILE
def convert_to_sql_string_format(row, columns, schema_name, table_name):
    lst_of_values = row.to_list()
    columns_sql_format = ', '.join(f'[{item}]' for item in columns)
    values_sql_format = ', '.join(f'{value}' for value in lst_of_values)
    str_blueprint = f"INSERT [{schema_name}].[{table_name}] ({columns_sql_format}) VALUES ({values_sql_format})"
    return str_blueprint

# Function to apply 'func' to a DataFrame or Series 'data'
def apply_func(data, func):
    return data.apply(func)

# Function to parallelize pandas apply
def parallel_apply(data, func, num_partitions=STANDARD_NUMBER_OF_PARTITIONS):
    # Split DataFrame into chunks
    data_split = np.array_split(data, num_partitions)
    
    with ProcessPoolExecutor() as executor:
        # Apply the function to each partition in parallel
        results = list(executor.map(apply_func, data_split, [func]*num_partitions))
    
    # Concatenate the results back into a single DataFrame or Series
    return pd.concat(results)

def translate_df_columns(df):
    columns_to_translate = ['Name', 'Description']

    for column in columns_to_translate:
        print(f"Translating column: {column}")
        df[column] = parallel_apply(df[column], translate_text_with_delay)
        # df[column] = df[column].apply(translate_text_with_delay)
    return df

def main():
    now = dt.datetime.now()
    print("Iniciando script...")
    print("Lendo arquivo a ser traduzido...")
    with open('teste 500 linhas.sql', 'r', encoding='utf-8') as f:
        raw_data = f.readlines()
    raw_data = [item for item in raw_data if item!='\n']
    print("Coletando nome das colunas...")
    columns = fetch_column_names(raw_data[0])
    print("Coletando nome do schema e da tabela de dados...")
    schema_name, table_name = fetch_schema_and_table_names(raw_data[0])
    df = permeate_values_on_df(columns, raw_data)
    df_translated = translate_df_columns(df)
    df_translated.to_excel("translated_df_excel.xlsx")

    df_sql_formatted_translated = pd.DataFrame(columns=["Translated_SQL"])
    df_sql_formatted_translated["Translated_SQL"] = df_translated.apply(convert_to_sql_string_format, axis=1, columns=columns, schema_name=schema_name, table_name=table_name)
    df_sql_formatted_translated.to_csv("translated_data.sql", sep="\n", index=False, header=False)

    if not manually_translate:
        manually_translate.append("NO FAILED TRANSLATIONS...")
    df_translation_failed = pd.DataFrame(manually_translate, columns=["Translation_Failed"])
    df_translation_failed.to_csv("failed_translations.txt", sep="\n")
    end = dt.datetime.now()
    print(f"O Scrpit levou {(end-now)} para traduzir {len(df_translated.index)} linhas")

if __name__ == "__main__":
    main()