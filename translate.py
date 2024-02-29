import re
import time
import numpy as np
import pandas as pd
import translators as ts
from googletrans import Translator, LANGUAGES

translator = Translator()

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
    return df_new

# def translate(text):

# Function to translate text with delay to avoid rate limits
def translate_text_with_delay(text, delay=1):
    raw_unstranslated_text = re.findall(r'\'(.*?)\'', text)
    translated_text = ''
    if len(raw_unstranslated_text[0]):
        translated_text_en = ts.translate_text(query_text=raw_unstranslated_text[0], translator = 'bing', from_language = 'auto', to_language = 'en')
        translated_text = ts.translate_text(query_text=translated_text_en, translator = 'bing', from_language = 'en', to_language = 'pt')
        time.sleep(delay)  # Wait for a specified delay (in seconds) between requests
    original_format_translated_text = f"N'{translated_text}'"
    return original_format_translated_text

def translate_df_columns(df):
    columns_to_translate = ['Name', 'Description']

    for column in columns_to_translate:
        df[column] = df[column].apply(translate_text_with_delay)
    return df

def main():
    with open('teste 500 linhas.sql', 'r', encoding='utf-8') as f:
        raw_data = f.readlines()
    raw_data = [item for item in raw_data if item!='\n']
    columns = fetch_column_names(raw_data[0])
    df = permeate_values_on_df(columns, raw_data)
    df_translated = translate_df_columns(df.iloc[0:3])
    print(df.iloc[0:3])
    print(df_translated.iloc[0:3])

if __name__ == "__main__":
    main()