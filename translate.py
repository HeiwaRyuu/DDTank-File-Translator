import re
import time
import numpy as np
import pandas as pd
import datetime as dt
import translators as ts
from langdetect import detect
from concurrent.futures import ProcessPoolExecutor
import warnings
warnings.filterwarnings(action='ignore', category=FutureWarning)

MAX_ATTEMPTS = 1
STANDARD_SLEEP_TIME = 1
STANDARD_NUMBER_OF_PARTITIONS = 20
manually_translate = []

def restructure_sql_into_datarframe(raw_data):
    transformed_lines = []
    current_statement = ""
    for line in raw_data:
        # Check if the line starts with "GO", treat it as a separate command
        if line.strip().startswith("GO"):
            # Append the current statement before adding "GO" if there's an ongoing statement
            if current_statement:
                transformed_lines.append(current_statement.rstrip('\n'))
                current_statement = ""
            transformed_lines.append("GO")  # Append "GO" as a separate line
        elif line.strip().startswith("INSERT"):
            # Start of a new statement
            if current_statement:  # If there's an ongoing statement, append it first
                transformed_lines.append(current_statement.rstrip('\n'))
            current_statement = line
        else:
            # Continuation of the current statement
            if current_statement[-1] != "\n":
                current_statement += "\n" + line.strip()
            else:
                current_statement += line.strip()

    # MAKING SURE LAST STATEMENT ID ADDED
    if current_statement:
        transformed_lines.append(current_statement.rstrip('\n'))

    return transformed_lines

def fetch_schema_and_table_names(row):
    row = row[0:row.find('(')]
    row_lst = row.split('.')
    schema_name = row_lst[0][row_lst[0].find('[')+1:row_lst[0].find(']')]
    table_name = row_lst[1][row_lst[1].find('[')+1:row_lst[1].find(']')]
    return schema_name, table_name

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
    # df_new = df_new.dropna() ## REMOVING ANY COLUMN THAT HAS EMPTY (None) VALUES
    return df_new

def translate(raw_untranslated_text, delay=STANDARD_SLEEP_TIME):
    translated_text = ''
    if len(raw_untranslated_text[0])>0:
        translated_text_en = ts.translate_text(query_text=raw_untranslated_text[0], translator = 'bing', from_language = 'auto', to_language = 'en')
        translated_text = ts.translate_text(query_text=translated_text_en, translator = 'bing', from_language = 'en', to_language = 'pt')
        translated_text = ts.translate_text(query_text=translated_text_en, translator = 'bing', from_language = 'auto', to_language = 'pt')
        translated_text = translated_text.replace('\'', '').replace('\"', '')
    return f"N'{translated_text}'"

# Function to translate text with delay to avoid rate limits
def translate_text_with_delay(text, **kwargs):
    try:
        raw_untranslated_text = re.findall(r'\'(.*?)\'', text, re.DOTALL)
    except Exception as e:
        ## THIS ONLY HAPPENS IF NONE IS PRESENT ON DATAFRAME COLUMN, OTHERWISE IT RETURNS THE REGULAR TEXT
        print(f"Exception when parsing regex on untranslated text {text}: Exception: {e}")
        return "ADD_GO_COMMAND" ## ADDING A TAG TO IDENTIFY WHICH ROW TO REPLACE WITH "GO" COMMAND
    
    ## IF TEXT IS ALREADY IN PORTUGUESE, IGNORE THE TRANSLATION EVENT
    try:
        if len(raw_untranslated_text[0])>0:
            if(detect(raw_untranslated_text[0])=="pt"):
                return text
    except Exception as e:
        print(f"TEXT === {text}")
        print(f"EXCESSÃO === {e}")
    
    attempts = 0
    while(attempts < MAX_ATTEMPTS):
        try:
            print(f"Translating text: {raw_untranslated_text}...")
            original_format_translated_text = translate(raw_untranslated_text)
            return original_format_translated_text
        except Exception as e:
            attempts += 1
            print(f"Exception while translating code: Exception: {e}... Trying again... Attempt number {attempts}")
            # time.sleep(STANDARD_SLEEP_TIME*20)
    if attempts >= MAX_ATTEMPTS:
        print(f"Max attempts on the row with the following text: {text}")
        manually_translate.append(text)
    
    return text

# RECONSTRUCTING SQL TRANSLATED FILE
def convert_to_sql_string_format(row, columns, schema_name, table_name):
    if "ADD_GO_COMMAND" in row["Name"]:
        return "GO" ## REPLACING LINE WITH "GO" COMMAND IF "ADD_GO_COMMAND" TAG IS PRESENT ON ROW
    lst_of_values = row.to_list()
    columns_sql_format = ', '.join(f'[{item}]' for item in columns)
    values_sql_format = ', '.join(f'{value}' for value in lst_of_values)
    str_blueprint = f"INSERT [{schema_name}].[{table_name}] ({columns_sql_format}) VALUES ({values_sql_format})"
    return str_blueprint

# Function to apply 'func' to a DataFrame or Series 'data'
def apply_func(data, func, kwargs):
    return data.apply(func, **kwargs)

# Function to parallelize pandas apply
def parallel_apply(data, func, num_partitions=STANDARD_NUMBER_OF_PARTITIONS, **kwargs):
    # Split DataFrame into chunks
    data_split = np.array_split(data, num_partitions)
    
    with ProcessPoolExecutor() as executor:
        # Apply the function to each partition in parallel
        if kwargs:
            results = list(executor.map(apply_func, data_split, [func]*num_partitions, [kwargs]*num_partitions))
        else:
            results = list(executor.map(apply_func, data_split, [func]*num_partitions, [{"fake_kwarg":True}]*num_partitions))
    
    # Concatenate the results back into a single DataFrame or Series
    return pd.concat(results)

def translate_df_columns(df):
    columns_to_translate = ['Name', 'Description']

    for column in columns_to_translate:
        print(f"Translating column: {column}")
        df[column] = parallel_apply(df[column], translate_text_with_delay)
    return df


def main():
    now = dt.datetime.now()
    print("Iniciando script...")
    print("Lendo arquivo a ser traduzido...")
    file_name = 'teste.sql'
    with open(file_name, 'r', encoding='utf-8') as f:
        raw_data = f.readlines()
    # raw_data = [item for item in raw_data if item!='\n']
    raw_data = restructure_sql_into_datarframe(raw_data)
    print("Coletando nome das colunas...")
    columns = fetch_column_names(raw_data[0])
    print("Coletando nome do schema e da tabela de dados...")
    schema_name, table_name = fetch_schema_and_table_names(raw_data[0])
    df = permeate_values_on_df(columns, raw_data)
    df.to_excel("CHECK.xlsx")
    df_translated = translate_df_columns(df)
    

    df_sql_formatted_translated = pd.DataFrame(columns=["Translated_SQL"])
    kwargs = {"axis":1, "columns":columns, "schema_name":schema_name, "table_name":table_name} ## PASSING KWARGS SO WE CAN PARRALELIZE EVERYTHING
    df_sql_formatted_translated["Translated_SQL"] = parallel_apply(df_translated, convert_to_sql_string_format, **kwargs)
    with open(f"translated_data-{file_name.split('.')[0]}.sql", "w+", encoding="utf-8") as f:
        f.write("\n".join(df_sql_formatted_translated["Translated_SQL"]))
    df_translated = df_translated.dropna()
    df_translated.to_excel(f"translated_df_excel-{file_name.split('.')[0]}.xlsx")

    if not manually_translate:
        manually_translate.append("NO FAILED TRANSLATIONS...")
    df_translation_failed = pd.DataFrame(manually_translate, columns=["Translation_Failed"])
    df_translation_failed.to_csv(f"failed_translations-{file_name.split('.')[0]}.txt", sep="\n")
    end = dt.datetime.now()
    print(f"O Scrpit levou {(end-now)} para traduzir {len(df_translated.index)} linhas")

if __name__ == "__main__":
    main()