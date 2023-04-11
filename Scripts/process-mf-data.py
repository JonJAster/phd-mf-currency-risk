import numpy as np
import pandas as pd
import statsmodels.api as sm
import scipy.stats as stats
import re
import os
import progressbar as pb

COUNTRY_GROUPS = ["lux", "kor", "usa", "can-chn-jpn", "irl-bra", "gbr-fra-ind",
                  "esp-tha-aus-zaf-mex-aut-che", "other"]

# Define Assisting Functions
def load_data(filename_base, country_group_code, series_type, value_name=None,\
              exp_dtype=None, cs_dates=None):
    """
    Load one data table for one country.

    Parameters
    ----------
    filename_base : str
        The base string of the name of the data to be loaded.
    country_group_code : str
        A filename suffix that selects the correct country group.
    series_type : {"panel", "cross"}
        An indicator for whether the table to be read is panel data or
        cross-sectional data.
    value_name : str, default None
        For panel data, a name for the time-series data component must
        be given
    exp_dtype : type, default None
        If provided (for panel data only), sets the datatype of all
        columns to be loaded except for the three left-most columns.
    cs_dates: str or sequence of str, default None
        If provided (for cross-sectional data only), indicates which
        columns should be parsed as dates (using the new names provided
        in cs_columns if any are given)

    Returns
    -------
    df_return : DataFrame
        The loaded and formatted data to be read.

    """
    
    # --- SCRUB INPUTS ---
    # Place solitary cs_dates input into a list.
    if isinstance(cs_dates, str):
        cs_dates = [cs_dates]
    
    # --- LOAD DATA ---
        
    # Declare the filename of the file to be loaded
    filename = (
        "./data/prepared/mutual-funds/{base}/mf_{base}_{country}.csv"
        .format(base=filename_base,country=country_group_code)
    )
    if series_type == "cross":
        # Read and combine the partial csv files
        df_return = pd.read_csv(filename, parse_dates=cs_dates, dayfirst=True)
    elif series_type == "panel":
        if value_name is None:
            raise ValueError("Parameter value_name must be provided for panel data.")
        
        # Pull out the columns names of the csv for use in declaring
        # the start date and in setting explicit datatypes
        col_names = pd.read_csv(filename, nrows=0).columns
        
        if exp_dtype is not None:
            # Pull out column names to declare which columns should be
            # typed in the next read
            dict_dtypes = dict(zip(col_names[3:],
                                   [exp_dtype]*(col_names.size-3)))
            
            # Read the csv
            df_return = pd.read_csv(filename, dtype=dict_dtypes)
        else:
            df_return = pd.read_csv(filename)
    
        # Remove the first column (Morningstar Direct doesn't allow you
        # to drop Fund Name from the data)
        df_return = df_return.iloc[:, 1:].copy()
        # Rename panel data columns to datetime values
        dates = pd.Series(pd.to_datetime(col_names[3:]))
        df_return.columns = pd.concat([pd.Series(["fundid", "secid"]), dates])
        # Drop date columns that have no non-nan entries
        df_return.dropna(axis=1, how="all", inplace=True)

        # Reshape panel data to tall format
        df_return = df_return.melt(id_vars=["fundid","secid"],
                                   var_name="date", value_name=value_name)
        
    else:
        raise ValueError("Parameter series_type must be either 'panel' "
                         "or 'cross'.")
    
    # Return the loaded dataframe
    return df_return

def panelmerge(dflist, how="outer"):
    """
    Merge panel data into a single DataFrame.
    Parameters 
    ----------
    dflist : sequence of DataFrames
        A list of dataframes containing panel data to be merged.
    join : {"outer", "inner", "left", "right", "cross"}, default "outer"
        The join parameter to fed into the call to the merge call.
        
    Returns
    -------
    df_return : DataFrame
        The merged DataFrame.
    """

    # Start a progress bar
    bar = pb.ProgressBar(max_value = 2)
    bar.update(0)
    for i in dflist:
        # For the first dataframe, set the return variable to that
        # DataFrame. For every subsequent DataFrame, merge it with the
        # existing return variable.
        if i is dflist[0]:
            df_return = i.copy()
        else:
            df_return = df_return.merge(i, on=["fundid", "secid", "date"],
                                        how=how)
            bar.update(bar.value + 1)
    
    return df_return

def trim_nans(df_in, id_level="secid"):
    """
    For a DataFrame of mutual fund data, drop any observations for a
    fund class before the first nonmissing observation of gross return
    and after the last nonmissing observation.
    
    Parameters
    ----------
    df_in : DataFrame
        DataFrame to be trimmed.
    id_level : {"secid", "fundid"}, default "secid"
        Inner-most level of ID still in the DataFrame
    """
    
    # First, forward and back fill values of gross return. Then, delete
    # any observations for which either of the fills is null, because
    # observations before the first return will have null forward fill
    # values, and observations after the final return will have null
    # back fill values.
    df_in["before_first_ret_flag"] = (
        df_in.groupby(id_level).ret_gross_m.ffill()
    )
    df_in["after_final_ret_flag"] = (
        df_in.groupby(id_level).ret_gross_m.bfill()
    )
    
    df_return = (
        df_in.copy()
             .dropna(subset=["before_first_ret_flag","after_final_ret_flag"],
                     how="any")
             .drop(["before_first_ret_flag", "after_final_ret_flag"], axis=1)
    )

    return df_return

def agg_verify(df_in, column_names, return_concurrents=False):
    """
    Verify that secids can be aggregated into one fundid on each date.
    This function will take a list of column names and check to make
    sure there is at most one value in that column across all secids
    that share a fundid and a date. The function will return a list of
    the columns that contain discrepancies, or an empty list if none do
    
    Parameters
    ----------
    df_in : DataFrame
        The DataFrame to verify
    columns_names : sequence of str
        The column names to check
    return_concurrents : bool
        If True, the function will return the df_concurrents
        DataFrame instead of the list of discrepancies.
        
    Returns
    -------
    ret_list : list
        A list of discrepancies
    
    or
    
    df_concurrents : DataFrame
        A DataFrame containing the number of unique values of each of
        the input columns across all of their fundid-date pairs.
    """
    # If only one column name is given, add it to a list.
    if isinstance(column_names, str):
        column_names = [column_names]
    
    # Calculate the maximum number of unique values in each of the
    # testable columns across all rows that share a fundid and date.
    df_concurrents = (
        df_in.groupby(["fundid", "date"])[column_names].nunique()
    )

    df_maxconcurrents = df_concurrents.max()
    
    # Initialise the return variable as an empty list.
    ret_list = []
    
    # Loop through every column name, check the max number of
    # concurrent unique values, and add a discrepancy if that number
    # is more than 1.
    for i in df_maxconcurrents.index:
        if df_maxconcurrents[i] == 0:
            print("Warning: "+i+" contains no usable observations")
        elif df_maxconcurrents[i] > 1:
            ret_list += [i]
    
    if return_concurrents:
        output = df_concurrents
    else:
        output = ret_list
        
    return output

def main(country_group_code, currency_type, raw_ret_only, polation_method, strict_eq,
         exc_finre, inv_targets, inc_agefilter):
    """
    Parameters
    ----------
    country_group_code : str
        The country group code to load data for.
    currency_type : ["local", "usd"]
        The currency group that returns are denominated in.
    raw_ret_only : bool
        If True, raw gross returns only will be used in the final dataset. If False,
        missing values of gross returns will be calculated using net returns and
        representative costs where available.
    polation_method : [False, "interpolate", "extrapolate", "both"]
        If "interpolate", net asset values will be interpolated. If "extrapolate", net
        assets values will be extrapolated. If "both", net asset values will be both
        extrapolated and interpolated. If False, neither extrapolation or interpolation
        will occur.
    strict_eq : bool
        If True, only Morningstar categories classified as being "strict" equity categories
        will be included in the final dataset.
    exc_finre : bool
        If True, funds classified as investing primarily in financial, infrastructure and
        real estate securities will be excluded.
    inv_targets : bool
        If True, the resultant DataFrame will include information about the investment
        target of each fund, at levels of MSCI class, region, group and country.
    inc_agefilter : bool
        If True, additional DataFrames will be saved that include only age-filtered mutual
        fund data, as well as the non-filtered DataFrames.
    """    

    with pb.ProgressBar(max_value=6) as bar:
        bar.update(0)
        # Fund information
        df_mfinfo = load_data("info", country_group_code, series_type="cross",
                              cs_dates = ["inception-date"])
        print(df_mfinfo.head()) ## DELETE LATER
        bar.update(1)

        # Gross monthly returns
        df_mfret_g = load_data(f"{currency_type}-monthly-gross-returns", country_group_code,
                               series_type="panel", value_name="ret_gross_m",
                               exp_dtype=np.float64)
        print(df_mfret_g.head()) ## DELETE LATER
        bar.update(2)

        # Net monthly returns
        df_mfret_n = load_data(f"{currency_type}-monthly-net-returns", country_group_code,
                               series_type="panel", value_name="ret_net_m",
                               exp_dtype=np.float64)
        print(df_mfret_n.head()) ## DELETE LATER
        bar.update(3)

        # Monthly net assets
        df_mfna = load_data("monthly-net-assets", country_group_code,
                            series_type="panel", value_name="net_assets",
                            exp_dtype=np.float64)
        print(df_mfna.head()) ## DELETE LATER
        bar.update(4)

        # Monthly representative costs
        df_mfcosts = load_data("monthly-costs", country_group_code,
                            series_type="panel", value_name="rep_costs",
                            exp_dtype=np.float64)
        print(df_mfcosts.head()) ## DELETE LATER
        bar.update(5)

        # Monthly Morningstar category
        df_mfcat = load_data("monthly-morningstar-category", country_group_code,
                            series_type="panel", value_name="morningstar_category",
                            exp_dtype=object)
        print(df_mfcat.head()) ## DELETE LATER
        bar.update(6)

main(COUNTRY_GROUPS[0], "local", False, False, False, False, False, True)