import env
from env import username
import pandas as pd
import os
from env import get_db_url

# from our acquire.py:
def get_connection(db, user=env.username, host=env.host, password=env.password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    
def get_mallcustomer_data():
    df = pd.read_sql('SELECT * FROM customers;', get_connection('mall_customers'))
    return df.set_index('customer_id')

            
import pandas as pd
import os
from env import get_db_url

def get_connection(db, user=env.username, host=env.host, password=env.password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def acquire_zillow():
    '''
    This function checks to see if zillow.csv already exists,
    if it does not, one is created
    '''
    #check to see if zillow.csv already exist
    if os.path.isfile('zillow.csv'):
        df = pd.read_csv('zillow.csv', index_col=0)
    else:
        url = get_db_url('zillow')
        df = pd.read_sql('''SELECT prop.*,
                           pred.logerror,
                           pred.transactiondate,
                           air.airconditioningdesc,
                           arch.architecturalstyledesc,
                           build.buildingclassdesc,
                           heat.heatingorsystemdesc,
                           landuse.propertylandusedesc,
                           story.storydesc,
                           construct.typeconstructiondesc
                    FROM   properties_2017 prop
                           INNER JOIN (SELECT parcelid,
                                              logerror,
                                              Max(transactiondate) transactiondate
                                       FROM   predictions_2017
                                       GROUP  BY parcelid, logerror) pred
                                   USING (parcelid)
                           LEFT JOIN airconditioningtype air USING (airconditioningtypeid)
                           LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid)
                           LEFT JOIN buildingclasstype build USING (buildingclasstypeid)
                           LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid)
                           LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid)
                           LEFT JOIN storytype story USING (storytypeid)
                           LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid)
                    WHERE  prop.latitude IS NOT NULL
                           AND prop.longitude IS NOT NULL AND transactiondate <= '2017-12-31'
                            ;''', url)
        #creates new csv if one does not already exist
        df.to_csv('zillow.csv')
    return df


def nulls_by_col(df):
    num_missing = df.isnull().sum()
    rows = df.shape[0]
    prcnt_miss = num_missing / rows * 100
    cols_missing = pd.DataFrame({'num_rows_missing': num_missing, 'percent_rows_missing': prcnt_miss})
    return cols_missing

def nulls_by_row(df):
    num_missing = df.isnull().sum(axis=1)
    prcnt_miss = num_missing / df.shape[1] * 100
    rows_missing = pd.DataFrame({'num_cols_missing': num_missing, 'percent_cols_missing': prcnt_miss})\
    .reset_index()\
    .groupby(['num_cols_missing', 'percent_cols_missing']).count()\
    .rename(index=str, columns={'customer_id': 'num_rows'}).reset_index()
    return rows_missing


    
def remove_columns(df, cols_to_remove):
    df = df.drop(columns=cols_to_remove)
    return df

def handle_missing_values(df, prop_required_columns=0.5, prop_required_row=0.75):
    threshold = int(round(prop_required_columns * len(df.index), 0))
    df = df.dropna(axis=1, thresh=threshold)
    threshold = int(round(prop_required_row * len(df.columns), 0))
    df = df.dropna(axis=0, thresh=threshold)
    return df

# combining everything in a cleaning function:

def data_prep(df, cols_to_remove=[], prop_required_column=0.5, prop_required_row=0.75):
    df = remove_columns(df, cols_to_remove)
    df = handle_missing_values(df, prop_required_column, prop_required_row)
    return df

def get_upper_outliers(s, k=1.5):
    q1, q3 = s.quantile([.25, 0.75])
    iqr = q3 - q1
    upper_bound = q3 + k * iqr
    return s.apply(lambda x: max([x - upper_bound, 0]))

def add_upper_outlier_columns(df, k=1.5):
    for col in df.select_dtypes('number'):
        df[col + '_outliers_upper'] = get_upper_outliers(df[col], k)
    return df

def transpose_count_nulls(df):
    null_list = []
    percent_nulls_list = []
    
    #make a list of the null counts for each columns
    for col in df.columns:
        value = df[col].isnull().sum()
        null_list.append(value)
    
    #make a list of the null percentages
    total_rows = df.shape[0]
    
    for col in df.columns:
        col_nulls = df[col].isnull().sum()
        percent_nulls_list.append(round(col_nulls / total_rows, 2))

    #transpose the dataframe
    df = df.T
    
    #insert the lists of null counts and null percents into the dataframe
    df.insert(0, 'null_percents', percent_nulls_list)
    df.insert(0, 'null_values', null_list)
    
    df = df[['null_values', 'null_percents']]
    
    return df