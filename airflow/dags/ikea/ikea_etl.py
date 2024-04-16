import requests, re, time, random, asyncio, aiohttp, json, os, datetime
from rich import print
import pandas as pd
from datetime import datetime
from functions import get_total_number_of_results, split_total_into_batches, get_payloads, fetch_all

ikea_url = "https://www.ikea.com/se/sv/new/new-products/"
result_name = f"ikea_new_products_{datetime.now().strftime('%m_%d_%Y_%H:%M:%S')}.csv"

def extract_raw_data(url) -> pd.DataFrame:
    keyword = re.search(r'-([^-\s/]+)\/?$', url).group(1)
    api_request_url = "https://sik.search.blue.cdtapps.com/se/sv/search?c=listaf"

    total_number_of_results = get_total_number_of_results(keyword)
    batches = split_total_into_batches(total_number_of_results)

    payloads = get_payloads(keyword, batches)
    results = asyncio.run(fetch_all(api_request_url, payloads))

    products = [product for sublist in results if sublist for product in sublist]

    df = pd.json_normalize(products)
    return df

df_ikea = extract_raw_data(ikea_url)

def transform_raw_ikea_data(raw_data_df:pd.DataFrame) -> pd.DataFrame:
    raw_data_df['Color name'] = raw_data_df['colors'].apply(lambda x: x[0].get('name') if x and 'name' in x[0] else None)
    raw_data_df['Color hex'] = raw_data_df['colors'].apply(lambda x: x[0].get('hex') if x and 'hex' in x[0] else None)

    raw_data_df = raw_data_df.loc[:,['pipUrl', 'id', 'name', 'typeName', 'mainImageUrl', 'ratingValue', 'ratingCount', 'salesPrice.current.wholeNumber', 'Color name', 'Color hex', 'mainImageAlt']]
    raw_data_df.rename(columns = {'pipUrl':'URL', 'id':'ID', 'name':'Name', 'typeName':'Type', 'mainImageUrl':'Image URL', 'ratingValue':'Rating value',
                            'ratingCount':'Rating count', 'salesPrice.current.wholeNumber':'Price', 'mainImageAlt':'Description'}, inplace=True)
    clean_data_df = raw_data_df.reset_index(drop=True)

    return clean_data_df

df_ikea_clean = transform_raw_ikea_data(df_ikea)
df_ikea_clean.to_csv(result_name)