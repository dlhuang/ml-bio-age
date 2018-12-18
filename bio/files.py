import pandas as pd

import requests
import xport

from bio.columns import input_col_map, output_col_map, all_cols

NHANES_BASE_URL = 'https://wwwn.cdc.gov/Nchs/Nhanes/'
NHANES_SUFFIX = '.XPT'

input_files = {
    '1999-2000': [
	'LAB25',
	'DEMO',
	'LAB13',
	'LAB18',
	'LAB06',
	'LAB13AM',
    ],
    '2001-2002': [
        'L25_B',
        'L25_2_B',
        'DEMO_B',
        'L40_B',
        'L13AM_B',
        'L13_B',
        'L13_2_B',
        'L40_2_B',
        'L06_2_B',
        'L40FE_B',
    ],
    '2003-2004': [
        'L25_C',
        'DEMO_C',
        'L40_C',
        'L13_C',
        'L06COT_C',
        'L06BMT_C',
        'L06TFR_C',
        'L13AM_C',
        'L13AM_C',
        'L40FE_C',
    ],
    '2005-2006': [
        'CBC_D',
        'DEMO_D',
        'HDL_D',
        'TRIGLY_D',
        'TCHOL_D',
        'BIOPRO_D',
        'HCY_D',
        'FERTIN_D',
        'FETIB_D',
    ],
    '2007-2008': [
        'CBC_E',
        'DEMO_E',
        'BIOPRO_E',
        'HDL_E',
        'TRIGLY_E',
        'TCHOL_E',
        'FERTIN_E',
    ],
    '2009-2010': [
        'CBC_F',
        'BIOPRO_F',
        'DEMO_F',
        'HDL_F',
        'TRIGLY_F',
        'TCHOL_F',
        'FERTIN_F',
    ],
    '2011-2012': [
        'CBC_G',
        'DEMO_G',
        'HDL_G',
        'TRIGLY_G',
        'TCHOL_G',
        'BIOPRO_G',
    ],
    '2013-2014': [
        'CBC_H',
        'DEMO_H',
        'HDL_H',
        'TRIGLY_H',
        'TCHOL_H',
        'BIOPRO_H',
    ],
    '2015-2016': [
        'CBC_I',
        'DEMO_I',
        'HDL_I',
        'TCHOL_I',
        'BIOPRO_I',
    ]
}

def get_fname(fname):
    return f'{fname}{NHANES_SUFFIX}'

def download(datadir, input_files=input_files):
    for (year, files) in input_files.items():
        for fname in files:
            fname = get_fname(fname)
            ofname = datadir / fname
            if ofname.exists():
                print(f'Skipping {ofname} (file exists)')
            else:
                url = f'{NHANES_BASE_URL}{year}/{fname}'
                print(f'Downloading {url} to {ofname}')
                r = requests.get(url, stream=True)
                if r.status_code == 200:
                    with open(ofname, 'wb') as f:
                        f.write(r.content)
                else:
                    raise FileNotFoundError(f'{url}')
    print('Done')

def get_df(datadir, fname, key):
    fname = get_fname(fname)
    df = xport.to_dataframe(open(datadir / fname, 'rb'))
    df.set_index(key, inplace=True)
    df.drop(df.columns.difference(all_cols), axis=1, inplace=True)
    df.rename(columns={**input_col_map, **output_col_map}, inplace=True)
    return df

def join_input(datadir, year, key='SEQN'):
    dfs = [get_df(datadir, fname, key) for fname in input_files[year]]
    df = pd.concat(dfs, ignore_index=True, sort=False)
    year = int(year.split('-')[0])
    df['YEAR'] = [year] * len(df)
    return df
