import numpy as np
import pandas as pd
import statsmodels.api as sm
import scipy.stats as stats
import re
import os
import multiprocessing
from pandas.tseries.offsets import MonthEnd
from datetime import datetime

COUNTRY_GROUPS = {0: "lux", 1: "kor", 2: "usa", 3: "can-chn-jpn", 4: "irl-bra",
                  5: "gbr-fra-ind", 6: "esp-tha-aus-zaf-mex-aut-che", 7: "other"}

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
        "data/mutual-funds/domicile-grouped/{base}/mf_{base}_{country}.csv"
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

        # Align dates to the end of the month
        df_return.date = df_return.date + MonthEnd(0)
        
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
    for i in dflist:
        # For the first dataframe, set the return variable to that
        # DataFrame. For every subsequent DataFrame, merge it with the
        # existing return variable.
        if i is dflist[0]:
            df_return = i.copy()
        else:
            df_return = df_return.merge(i, on=["fundid", "secid", "date"],
                                        how=how)
    
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

def polate_assets(df_in, how="interpolate", keep=False, retain_testdata=False):
    """
    This function either interpolates, extrapolates or both interpolates
    and extrapolates values of net assets within fund classes within a
    given dataframe.
    
    Parameters
    ----------
    df_in : DataFrame
        The data to be interpolated and/or extrapolated.
    how : {"interpolate", "extrapolate", "both", False},
        default "interpolate"
        The method to be used to add additional observations
    keep : bool, default False
        If True, the returned dataframe will retain the original net
        assets column
    retain_testdata : bool, default False
        If true, the returned dataframe will contain some additional
        columns useful for testing the accuracy of the algorithm
        
    Returns
    -------
    df_return : DataFrame
        A DataFrame containing the extended version of the original data.
    """
    
    # --- SCRUB INPUTS ---
    # Validate how, and immediately return if how=False.
    if not how:
        return df_in
    elif how not in ["interpolate", "extrapolate", "both"]:
        raise ValueError("how must be 'interpolate', 'extrapolate',"
                        "'both' or False.")
    
    # Validate keep
    if keep not in [True, False]:
        raise ValueError("keep must be True or False.")
        
    # Validate retain_testdata
    if retain_testdata not in [True, False]:
        raise ValueError("retain_testdata must be True or False.")
    
    # --- PREPARE DATA ---
    # Copy the input DataFrame, and drop all missing values of net
    # assets, ensuring that all observations are ordered by secid and
    # date. 
    df_assetobs = (
        df_in.copy().sort_values(by=["fundid", "secid", "date"])
                    .dropna(subset=["net_assets"])
                    .loc[:, ["secid", "date"]]
    )
    
    # --- PROCESS DATA ---

    # Assign a cumulative count column grouped by secid.
    df_assetobs["polation_id"] = df_assetobs.groupby("secid").cumcount()
    
    # Left merge the cumulative count column back into the original
    # DataFrame by secid and date, providing a unique ID to each
    # nonmissing observation of net_assets for a given secid.
    df_main = pd.merge(df_in, df_assetobs, on=["secid", "date"], how="left")

    # Back fill the polation ID within each secid. The purpose of this
    # backfilling is so to group together consecutive missing
    # observations with the next available nonmissing observation.
    df_main["polation_id"] = df_main.groupby("secid").polation_id.bfill()
    
    # All nan values of polation_id now occur either after the last
    # non-missing observation for a given fund class or where a fund
    # class has no non-missing values of net assets. The latter case
    # will fail when extrapolating anyway, so there is no need to
    # distinguish these two groups. Assign all missing values of
    # polation ID a value of "00", which will be used for extrapolation.
    df_main.polation_id.fillna("00", inplace=True)

    # Each polation group (except groups labelled 0) should be
    # associated with a base value of net assets which was the value
    # of net assets in the nonmissing observation immediately preceeding
    # the start of that interpolation group. This can be obtained by
    # forward filling net asset observations within a secid and then
    # shifting the forward-filled data forward by one place within that
    # secid. By forward filling net assets, we ensure that every missing
    # observation of net assets can see the most recent nonmissing
    # value. Still, the nonmissing value immediately following that
    # sequence of missing values cannot yet see the base value.
    # Additionally, the most recent nonmissing observation itself can
    # see the base value for that group (it's own value of net assets),
    # but does not need to see this value during the interpolation. So,
    # by shifting the forward-filled column forward once, the entire
    # polation group can see the most recent nonmissing value of net
    # assets.
    df_main["polation_group_asset_base"] = (
        df_main.groupby("secid").net_assets.ffill()
    )
    df_main["polation_group_asset_base"] = (
        df_main.groupby("secid").polation_group_asset_base.shift(1)
    )

    # For polation groups labelled 0, the asset target should be the net
    # assets of the first nonmissing observation. These polation groups
    # are now the only ones with a nan value for
    # polation_group_asset_base, so we achieve this by filling nan
    # values with a backfilled series of net assets within each secid.
    df_main.polation_group_asset_base.fillna(df_main.groupby("secid")
                                                    .net_assets
                                                    .bfill(), inplace=True)
    
    # Define a multiplicative net return column (1 + net return/100),
    # from which a cumulative net return column can be defined
    df_main["multret_net"] = df_main.ret_net_m/100 + 1
    
    # Multiplicative return will be NaN if net return is missing. To
    # ensure that interpolation completes even where some return values
    # are missing within a polation group, these NaN values will be
    # replaced by 1. The error this introduces into the interpolation
    # will be redistributed evenly across the group when the discrepancy
    # is accounted for. The error introduced into extrapolation in this
    # way would be significant and impossible to account for, so NaNs
    # will later be placed back into the cumulative return series for
    # extrapolation groups and propogated outwards away from the
    # nonmissing net asset series.
    df_main.multret_net.fillna(1, inplace=True)

    # Define a cumulative net return column within each polation group
    # for use in predicting net assets within that group absent the
    # impact of fund inflows and outflows.
    df_main["cumret_net"] = (
        df_main.groupby(["secid", "polation_id"]).multret_net.cumprod()
    )

    # Cumulative return can be multiplied by the asset base to arrive at
    # a predicted value for net assets excluding discrepancies for that
    # position for any polation group not labelled 0. To achieve the
    # same functionality for polation groups labelled 0, all cumulative
    # returns for that group should be divided by the value for
    # cumulative return of the first nonmissing net assets observation
    # for that secid. By dividing all cumulative returns by the same
    # value, we don't change the property that each return within that
    # group is equal to the cumulative return of the previous observation
    # multiplied by the net return of the current observation, but now
    # the cumulative product for the final observation in that group
    # (the observation with the first nonmissing value of net assets)
    # will be 1. So, multiplying net assets for each observation in that
    # group with the new cumulative return will result in a series of
    # net asset values that are each equal to the previous net assests
    # value grown by the current net return in a way that ends up
    # equalling the known value of net assets for the first nonmissing
    # observation.
        
    # Define a column to hold the next available value of multiplicative
    # return within each secid. Begin by copying the multiplicative
    # returns column but setting all values for observations with
    # missing net assets back to NaN so that the first non-NaN value
    # can be backfilled, then backfill the new column within each secid.
    df_main["cumret_divisor"] = np.where(df_main.net_assets.isnull(),
                                        np.nan, df_main.cumret_net)
    df_main["cumret_divisor"] = df_main.groupby(["secid"]).cumret_divisor.bfill()
    
    # Divide the cumulative return by the divisor value for that
    # observation only if the polation group id is 0.
    df_main["cumret_net"] = np.where(df_main.polation_id == 0,
                                    df_main.cumret_net/df_main.cumret_divisor,
                                    df_main.cumret_net)


    # Missing values of multiplicative return have previously been
    # replaced with 1, but this solution does not work for
    # extrapolation, so the first observation with a missing return in
    # a polation group labelled 0 or 00 should have its cumulative
    # return set to NaN, and this NaN value should be propogated outward
    # away from the series of nonmissing asset observations. It is
    # possible to propogate non-NaN values across NaN values, but not
    # the other way around, so we create a column to flag observations
    # that should be set to NaN and set the value of this flag to NaN
    # for non-flagged observations, then propogate the flag.
    df_main["stop_backextrapolation_flag"] = (
        np.where((df_main.polation_id == 0) & (df_main.ret_net_m.isnull()),
                1, np.nan)
    )
    df_main["stop_backextrapolation_flag"] = (
        df_main.groupby("secid").stop_backextrapolation_flag.bfill()
    )
    
    df_main["stop_forwardextrapolation_flag"] = (
        np.where((df_main.polation_id == "00") & (df_main.ret_net_m.isnull()),
                1, np.nan)
    )
    df_main["stop_forwardextrapolation_flag"] = (
        df_main.groupby("secid").stop_forwardextrapolation_flag.ffill()
    )
    
    # Now nullify flagged values of cumulative return
    df_main["cumret_net"] = (
        np.where((df_main.stop_backextrapolation_flag == 1)
                | (df_main.stop_forwardextrapolation_flag == 1),
                np.nan, df_main.cumret_net)
    )

    # Predict net assets absent the impact of fund inflows and outflows
    # (and other errors).
    df_main["net_assets_recalculated_exflows"] = (
        df_main.cumret_net * df_main.polation_group_asset_base
    )
    
    # The effect of fund inflows and outflows (and other errors if they
    # exist) across the time-series of a single polation group is given
    # by the ratio of the actual value of net assets in the final
    # observation for that polation group (which is always nonmissing)
    # and the calculated net assets excluding fund flows for that
    # observation. We define a discrepancy column which is equal to 1
    # (no change) both before the first nonmissing observation and
    # after the final nonmissing observation in each group, and then
    # calculate the required ratio, which will be NaN for any
    # observation with missing net assets. Finally, we backfill this
    # new column so that each polation group can see the discrepancy of
    # the next nonmissing observation.
    df_main["asset_discrepancy"] = (
        np.where((df_main.polation_id == 0) | (df_main.polation_id == "00"),
                1, df_main.net_assets/df_main.net_assets_recalculated_exflows)
    )
    df_main["asset_discrepancy"] = (
        df_main.groupby("secid").asset_discrepancy.bfill()
    )

    # Although the total discrepancy is known to each observation within
    # a polation group, for interpolations we must still determine how
    # far through the interpolation that observation appears so that the
    # impact of fund flows can be evenly distributed across the group.
    # For that we must know the total duration of the time series for
    # that interpolation group as well as where each observation appears
    # within the time series.

    # First, create a new dataframe that holds the duration of each
    # interpolation group time series, then merge that field back into
    # the original dataframe so that each observation within the group
    # can see that total.
    df_polationduration = (
        df_main.groupby(["secid", "polation_id"]).polation_id.count()
            .to_frame().rename(columns={"polation_id": "polation_duration"})
            .reset_index()
    )

    df_main = pd.merge(df_main, df_polationduration,
                    on=["secid", "polation_id"],
                    how="left").reset_index(drop=True)

    # Second, assign a cumulative count column within each polation
    # group.
    df_main["polation_progress"] = (
        df_main.groupby(["secid", "polation_id"]).cumcount() + 1
    )
    
    # Update recaluculated net assets by taking into acccount asset
    # discrepancies (for polation groups 0 and "00", where
    # asset_discrepancy is 1, this will have no effect)
    df_main["net_assets_recalculated"] = (
        df_main.net_assets_recalculated_exflows
        * df_main.asset_discrepancy**(df_main.polation_progress
                                    / df_main.polation_duration)
    )
    
    # If only "interpolation" is desired, nullify recalculated assets
    # over extrapolation groups. Likewise, if only "extrapolation" is
    # desired, nullify recalculated assets over interpolation groups.
    if(how=="interpolate"):
        df_main.loc[df_main.polation_id.isin([0,"00"]),
                    "net_assets_recalculated"] = np.nan
    elif(how=="extrapolate"):
        df_main.loc[~df_main.polation_id.isin([0,"00"]),
                    "net_assets_recalculated"] = np.nan
    
    # Ensure that the index of the inputted dataframe will align with
    # the index of the recalculated asset values.
    df_return = df_in.copy().reset_index(drop=True)

    # Retain the original net assets if either of keep or
    # retain_testdata is True.
    if keep or retain_testdata:
        df_return["net_assets_original"] = df_return.net_assets
    
    # If retain_testdata is True, also retain some of the intermediate
    # columns.
    if retain_testdata:
        df_return = (
            pd.concat([df_return, df_main.loc[:, ["multret_net",
                                                "cumret_net",
                                                "net_assets_recalculated",
                                                "net_assets_recalculated_exflows",
                                                "asset_discrepancy",
                                                "polation_id",
                                                "polation_duration",
                                                "polation_progress"]]],
                    axis=1)
        )
            
    # Fill null values of net_assets with the recalculated series.
    df_return.net_assets.fillna(df_main.net_assets_recalculated, inplace=True)
    
    return df_return

def process_fund_data(country_group_code, currency_type, raw_ret_only, polation_method,
                      strict_eq, exc_finre, inv_targets, inc_agefilter):
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
    # Grab the process ID and start time
    process_id = os.getpid()
    start_time = datetime.now()

    # Fund information
    df_mfinfo = load_data("info", country_group_code, series_type="cross",
                            cs_dates = ["inception_date"])

    # Gross monthly returns
    df_mfret_g = load_data(f"{currency_type}-monthly-gross-returns", country_group_code,
                            series_type="panel", value_name="ret_gross_m",
                            exp_dtype=np.float64)

    # Net monthly returns
    df_mfret_n = load_data(f"{currency_type}-monthly-net-returns", country_group_code,
                            series_type="panel", value_name="ret_net_m",
                            exp_dtype=np.float64)

    # Monthly net assets
    df_mfna = load_data("monthly-net-assets", country_group_code,
                        series_type="panel", value_name="net_assets",
                        exp_dtype=np.float64)

    # Monthly representative costs
    df_mfcosts = load_data("monthly-costs", country_group_code,
                        series_type="panel", value_name="rep_costs",
                        exp_dtype=np.float64)

    # Monthly Morningstar category
    df_mfcat = load_data("monthly-morningstar-category", country_group_code,
                        series_type="panel", value_name="morningstar_category",
                        exp_dtype=object)
    
    elapsed_time = datetime.now() - start_time
    print(f"Process {process_id} ({country_group_code}): Finished loading data ({elapsed_time} passed since process start)")
    
    # Combine panel data
    # Combine only the data that is required to remove unneccessary return rows, that being
    # "gross returns", "net returns" and "representative costs". Other data can be combined
    # later after unneccessary rows have been removed to save time in the merge.

    # Merge all returns data together. The resultant dataframe needs to be
    # sorted by date to enable removal of unnecessary rows.
    df_mfrets = (
        panelmerge([df_mfret_g, df_mfret_n, df_mfcosts]).sort_values(by="date")
    )

    # Clear unused memory
    del df_mfret_g, df_mfret_n, df_mfcosts

    # Recalculate monthly gross returns and correct for zero net return observations
    # (if raw_ret_only equals False).

    if not raw_ret_only:
        # Recalculate monthly gross returns using representative costs and
        # the monthly net return.
        df_mfrets["ret_gross_m_recalculated"] = (
            ((df_mfrets.ret_net_m/100 + 1)/(1-df_mfrets.rep_costs/100) - 1) * 100
        )

        # Fill missing values of monthly return with the recalculated
        # values.
        df_mfrets.ret_gross_m.fillna(df_mfrets.ret_gross_m_recalculated,
                                    inplace=True)
        
        # There are an inordinate number of ret_net_m observations equal to
        # exactly zero (125x more frequently occuring than the next most
        # common return value to 5 decimal places). Clearly, gross returns
        # in these cases should not be expected to have exactly offset
        # costs, so set gross returns also to zero for those observations.
        df_mfrets.loc[df_mfrets.ret_net_m == 0, "ret_gross_m"] = 0

    # Remove unnecessary rows
    df_mfrets = trim_nans(df_mfrets)

    # Merge the rest of the fund time-series data together
    df_mf = panelmerge([df_mfrets, df_mfna, df_mfcat], how="left")

    # Merge in country of domicile.
    df_mf = (
        df_mf.merge(df_mfinfo.loc[:,["secid", "domicile"]])
    )

    # Clear unused memory
    del df_mfrets, df_mfna, df_mfcat
    
    elapsed_time = datetime.now() - start_time
    print(f"Process {process_id} ({country_group_code}): Finished merging data ({elapsed_time} passed since process start)")

    # For the age-filtered funds dataset, we want eventually to only include
    # observations after the first 24 months, but many other filters need to
    # be applied first. Start tracking secid age now, so you can later take
    # the largest value in a given month for a fundid to be that fundid's
    # age. Return observations under 2 years old will be removed as one of
    # the final steps.
    if inc_agefilter:
        df_mf["age"] = df_mf.groupby("secid").cumcount()

    # Relabel countries as ISO codes
    ISO = {
        "Andorra": "AND", "Australia": "AUS", "Austria": "AUT", "Argentina": "ARG",
        "Bahamas": "BHS", "Bahrain": "BHR", "Belgium": "BEL", "Bermuda": "BMU",
        "Botswana": "BWA", "Brazil": "BRA", "British Virgin Islands": "VGB",
        "Canada": "CAN", "Cayman Islands": "CYM", "Chile": "CHL", "China": "CHN",
        "Colombia": "COL", "Curaçao": "CUW", "Czech Republic": "CZE",
        "Denmark": "DNK", "Estonia": "EST", "Finland": "FIN", "France": "FRA",
        "Germany": "DEU", "Gibraltar": "GIB", "Greece": "GRC", "Guernsey": "GGY",
        "Hong Kong": "HKG", "Iceland": "ISL", "India": "IND", "Indonesia": "IDN",
        "Ireland": "IRL", "Isle of Man": "IMN", "Israel": "ISR", "Italy": "ITA",
        "Japan": "JPN", "Jersey": "JEY", "Kuwait": "KWT", "Latvia": "LVA",
        "Lesotho": "LSO", "Liechtenstein": "LIE", "Lithuania": "LTU",
        "Luxembourg": "LUX", "Malaysia": "MYS", "Malta": "MLT",
        "Marshall Islands": "MHL", "Mauritius": "MUS", "Mexico": "MEX",
        "Namibia": "NAM", "Netherlands": "NLD", "New Zealand": "NZL",
        "Norway": "NOR", "Oman": "OMN", "Pakistan": "PAK", "Panama": "PAN",
        "Philippines": "PHL", "Poland": "POL", "Portugal": "PRT",
        "Puerto Rico": "PRI", "Qatar": "QAT", "Russian Federation": "RUS",
        "Samoa": "WSM", "San Marino": "SMR", "Saudi Arabia": "SAU",
        "Singapore": "SGP", "Slovenia": "SVN", "South Africa": "ZAF",
        "South Korea": "KOR", "Spain": "ESP", "St Vincent-Grenadines": "VCT",
        "Swaziland": "SWZ", "Sweden": "SWE", "Switzerland": "CHE", "Taiwan": "TWN",
        "Thailand": "THA", "Turkey": "TUR", "Ukraine": "UKR",
        "United Arab Emirates": "ARE", "United Kingdom": "GBR",
        "United States": "USA"
    }

    df_mf.domicile = df_mf.domicile.apply(lambda x: ISO[x])

    # Clean net assets
    # Filter out asset observations equal to zero.
    df_mf.loc[df_mf.net_assets == 0, "net_assets"] = np.nan
    
    # Run the interpolation/extrapolation function under the declared
    # method. Results must be resorted by date to allow for further
    # backfilling below.
    df_mf_pol = polate_assets(df_mf, how=polation_method, keep=True).sort_values(by="date")

    # Clear unused memory
    del df_mf

    # Eliminate non-equity fund classes
    # Read a list of accepted morningstar categories
    df_equity_categories = (
        pd.read_csv("./data/mappings/morningstar_categories.csv")
    )

    # If desired, merge Morningstar category fields into the main DataFrame.
    # Otherwise, just merge the equity definition categories.
    if inv_targets:
        df_mf_cat = df_mf_pol.merge(df_equity_categories,
                                    on="morningstar_category", how="left")
    else:
        df_mf_cat = (
            df_mf_pol.merge(df_equity_categories.loc[:,["morningstar_category",
                                                        "equity",
                                                        "strict_equity",
                                                        "fin_or_re"]],
                            on="morningstar_category", how="left")
        )

    # Clear unused memory.
    del df_mf_pol

    # Define a single effective equity classification category based on the
    # values of strict_eq and exc_finre.
    if strict_eq:
        df_mf_cat["equity_flag"] = df_mf_cat.strict_equity
    else:
        df_mf_cat["equity_flag"] = df_mf_cat.equity
        
    if exc_finre:
        df_mf_cat.equity_flag = df_mf_cat.equity_flag * (1-df_mf_cat.fin_or_re)

    # If any secid for a fundid-date pair has an equity classification,
    # then all secids with an ambiguous category (neither clearly equity
    # nor clearly non-equity - represented as np.nan) should be set to be
    # classified as equity. This catches situations where some secids are
    # classified as equity, but others, which hold mostly the same assets
    # but may include hedging positions, fall into a miscellaneous-type
    # category that is not clearly equity. Additionally, in rare cases
    # (only one instance in the originally tested dataset), there are some
    # unambiguously nonequity secids and unambiguously equity secids that
    # share a fundid and date. This can be taken as evidence of
    # uncertainty over that fund's status as an equity fund on that date,
    # and so all secids for that date-date are set to nonequity.

    # Define a value for each fundid-date pair equal to 1 if that pair
    # includes any equity-classified secids and no non-equity-classified
    # secids, 0 if it includes any non-equity-classified secids, and nan if
    # there are no nonmissing category observations across all secids. This
    # is achieved simply by calling min on the equity_flag column for each
    # pair.
    df_mf_eqfunds = (
        df_mf_cat.groupby(["fundid", "date"]).equity_flag.min().to_frame()
                .reset_index().rename(columns={"equity_flag":
                                                "override_eq_flag"})
    )

    # Merge this new column back into the main DataFrame. For each
    # fundid-date pair where this value is 1, all missing classifications
    # should be set to 1, and where this value is 0, all classifications
    # should be set to 0, but the value will not be 1 unless there are no
    # secids for that fundid-date pair that have a 0 for equity_flag, so
    # this can be achieved simply by overwriting all secid classifications
    # for all fundid-date pairs with that pair's value of override_eq_flag.
    df_mf_anyeq = df_mf_cat.merge(df_mf_eqfunds, on=["fundid", "date"], how="left")

    df_mf_anyeq.equity_flag = df_mf_anyeq.override_eq_flag

    # Eliminate return observations if the secid didn't belong to an
    # appropriate equity category in that month. This can't remove partial
    # fundid returns data as long as the verification above completed
    # successfully.
    df_mf_anyeq.ret_gross_m = np.where(df_mf_anyeq.equity_flag,
                                    df_mf_anyeq.ret_gross_m, np.nan)

    # After this filter, remove unnecessary rows once again (those that
    # exist for a secid before the first nonmissing observation of gross
    # returns, and after the last nonmissing observation - this will
    # eliminate entirely any secids that have had their full series deleted).
    # The method of trimming out non-equity returns has the benefit
    # of allowing returns to a fund that was at one point in time defined as
    # an equity category for the duration of definition as that category.
    df_mf_anyeq = trim_nans(df_mf_anyeq)

    # Aggregate into fund groups
    # Check to see if it's safe to aggregate secids under the same fundid by
    # ensuring that no two secids that share a fundid and date have
    # differing values of any of their categorical columns.

    if inv_targets:
        agg_check = agg_verify(df_mf_anyeq, ["inv_msci_class", "inv_region",
                                             "inv_group", "inv_country",
                                             "domicile"])
    else:
        agg_check = agg_verify(df_mf_anyeq, "domicile")
        
    if agg_check != []:
        for i in agg_check:
            print("Warning: Some fundid-date pairs contain at least two secids "
                  "that have different classifications of "+i+". ("+country_group_code+")")
            
    # Lag total net assets for use in weighting fund returns
    df_mf_anyeq["net_assets_m1"] = (
        df_mf_anyeq.groupby("secid").net_assets.shift(1)
    )

    # Calculate weights by summing net_assets across every secid for a given
    # fundid on a given date. Also calculate the number of fund classes for
    # a given fundid on a given date, as funds with only one class can be
    # aggregated even when fund_assets is missing. np.sum is called on the
    # values of net assets instead of simply calling the sum function to
    # force NaNs to propagate such that any missing value of net assets for
    # a class of a fund causes a missing value of fund_assets for that fund
    # on that date.
    df_fundassets = (
        df_mf_anyeq.groupby(["fundid", "date"])
                        .agg(fund_assets_m1=("net_assets_m1",
                                            lambda x: np.sum(x.values)),
                            num_classes=("secid", "count"))
    )

    df_weightedfunds = df_mf_anyeq.merge(df_fundassets,
                                            how="left", on=["fundid", "date"])

    # If the number of secids for a given fundid on a given date is exactly
    # one, set the weight of that secid's returns to 1, otherwise use net
    # asset weightings, which will be nan if any secid for that fundid-date
    # had a missing value of net assets.
    df_weightedfunds["return_weight"] = (
        np.where(df_weightedfunds.num_classes == 1, 1,
                df_weightedfunds.net_assets_m1/df_weightedfunds.fund_assets_m1)
    )

    # Weight all required columns
    df_weightedfunds.ret_gross_m = (df_weightedfunds.ret_gross_m
                                    * df_weightedfunds.return_weight)
    df_weightedfunds.ret_net_m = (df_weightedfunds.ret_net_m
                                * df_weightedfunds.return_weight)
    df_weightedfunds.rep_costs = (df_weightedfunds.rep_costs
                                * df_weightedfunds.return_weight)
    
    # Define the GroupBy.agg(...) keyword arguments as a dictionary based on
    # the run options chosen at the start of the notebook. Begin with the
    # columns that will always be aggregated. These are the return columns,
    # the representative cost column, the domicile columns, and an
    # "approximate" Morningstar category column that holds the modal secid
    # category for that fundid-date pair, or simply whichever category is
    # sorted first amongst all the modal categories if there are more than
    # one. This column is useful to determine a probable investment
    # category for that fund on that date at a glance.
    agg_dict = {
        "ret_gross_m": ("ret_gross_m", lambda x: np.sum(x.values)),
        "ret_net_m": ("ret_net_m", lambda x: np.sum(x.values)),
        "mean_costs": ("rep_costs", lambda x: np.sum(x.values)),
        "approx_morningstar_category": ("morningstar_category",
                                        lambda x: stats.mode(x)[0][0]),
        "domicile": ("domicile", "first")
    }

    # Add aggregate fund_age as the maximum age of all secids in a 
    # fundid-date pair only if age filtered returns data is desired.
    if inc_agefilter:
        agg_dict["fund_age"] = ("age", "max")
        
    # Always add a measure for fund assets, but the name of this measure
    # depends on the value set for polation_method.
    if polation_method in ["interpolate", "extrapolate", "both"]:
        assets_name = "net_assets_original"
    else:
        assets_name = "net_assets"

    agg_dict["fund_assets"] = (assets_name, lambda x: np.sum(x.values)) 

    # If investment targets are desired, all them all in.
    if inv_targets:
        agg_dict = {**agg_dict,
                    "inv_msci_class": ("inv_msci_class", "first"),
                    "inv_region": ("inv_region", "first"),
                    "inv_group": ("inv_group", "first"),
                    "inv_country": ("inv_country", "first")}

    # Aggregate all secids for the same fundid. Ensure that entires are
    # date sorted so that fund flows can be accurately calculated.
    df_mf_agg = (
        df_weightedfunds.groupby(["fundid", "date"]).agg(**agg_dict)
                        .reset_index()
                        .sort_values(by=["fundid", "date"])
    )
    
    elapsed_time = datetime.now() - start_time
    print(f"Process {process_id} ({country_group_code}): Finished aggregating funds ({elapsed_time} passed since process start)")

    # Lag fund_assets to calculate cash flows. Addtionally, lag the date
    # column to make sure changes in assets are only taken over a single
    # month.
    df_mf_agg[["date_m1", "fund_assets_m1"]] = (
        df_mf_agg.groupby("fundid")[["date", "fund_assets"]].shift(1)
    )

    # Define fund flows in month t as the ratio of fund_asset in t to
    # fund_assets in t-1 less the net returns to the fund at time t. If
    # month t is not one month after month t-1, set the fund flows to nan.
    df_mf_agg["fund_flow"] = (
        np.where(
            (df_mf_agg.date == df_mf_agg.date_m1+MonthEnd(1))
            & (df_mf_agg.fund_assets_m1 >= 10_000_000),
            df_mf_agg.fund_assets/df_mf_agg.fund_assets_m1
            - (1+df_mf_agg.ret_net_m/100),
            np.nan
        )
    )

    # There are extreme outliers of fund_flows on both the high and the
    # low end, which must be due to errors in either returns or net assets
    # data around those observations. Winsorise the fund_flows variable
    # at the 1st and 99th percentiles.
    df_mf_agg.fund_flow = (
        df_mf_agg.fund_flow.copy().clip(lower=df_mf_agg.fund_flow
                                                       .quantile(0.01,
                                                                 interpolation="lower"),
                                        upper=df_mf_agg.fund_flow
                                                       .quantile(0.99,
                                                                 interpolation="higher"))
    )

    # Correct for unrealistic mean costs
    # Mean costs can never be negative, and should never be more than 100%.
    df_mf_agg.loc[df_mf_agg.mean_costs < 0, "mean_costs"] = 0
    df_mf_agg.loc[df_mf_agg.mean_costs > 1, "mean_costs"] = 1

    # Correct for incubation bias with an age filter
    # If desired, filter out the first 2 years of observations using the
    # existing age field.
    if inc_agefilter:
        df_mf_filt = (
            df_mf_agg[df_mf_agg.fund_age >= 24].copy() #  Age is zero-indexed.
        )

        df_mf_agg.drop("fund_age", axis=1, inplace=True)
        df_mf_filt.drop("fund_age", axis=1, inplace=True)

    # Trim leading and trailing nans for the final time
    df_mf_agg = trim_nans(df_mf_agg.copy(), id_level="fundid")
    if inc_agefilter:
        df_mf_filt = trim_nans(df_mf_filt.copy(), id_level="fundid")

    # Filter out funds with fewer than 24 nonmissing monthly returns
    # Count the number of nonmissing returns for each fundid (for filtered
    # and unfiltered DataFrames if necessary).
    df_mf_retcounts_agg = (
        df_mf_agg.groupby("fundid").ret_gross_m.count().to_frame().reset_index()
                                .rename(columns={"ret_gross_m": "retcount"})
    )

    # Only retain fundids that have return counts greater than or equal to
    # 24.
    mature_fundids = (
        df_mf_retcounts_agg.loc[df_mf_retcounts_agg.retcount >= 24, "fundid"]
                        .values
    )

    df_mf = (
        df_mf_agg.loc[df_mf_agg.fundid.isin(mature_fundids)]
                .drop(["date_m1", "fund_assets_m1"], axis=1)
    )

    # Repeat for filtered DataFrame if necessary.
    if inc_agefilter:

        df_mf_retcounts_filt = (
            df_mf_filt.groupby("fundid").ret_gross_m.count().to_frame()
                    .reset_index().rename(columns={"ret_gross_m": "retcount"})
        )
        
        mature_fundids = (
            df_mf_retcounts_filt.loc[df_mf_retcounts_agg.retcount >= 24,
                                    "fundid"]
                                .values
        )

        df_mf_filt = (
            df_mf_filt.loc[df_mf_filt.fundid.isin(mature_fundids)]
                    .drop(["date_m1", "fund_assets_m1"], axis=1)
        )

    # Save refined data
    # Declare a filename suffix based on the particular run options that
    # were selected for this run.
    if currency_type == "local":
        folder_name = "local-rets"
    else:
        folder_name = "usd-rets"

    if not raw_ret_only:
        folder_name += "_gret-filled"

    if polation_method == "both":
        folder_name += "_na-int-exp"
    elif polation_method == "interpolate":
        folder_name += "_na-int"
    elif polation_method == "extrapolate":
        folder_name += "_na-exp"
        
    if strict_eq:
        if exc_finre:
            folder_name += "_eq-strict-exfinre"
        else:
            folder_name += "_eq-strict"
    elif exc_finre:
            folder_name += "_eq-exfinre"

    if inv_targets:
        folder_name += "_targets"

    folder_dir = (
        "./data/mutual-funds/post-processing/{}/initialised".format(folder_name)
    )

    if not os.path.exists(folder_dir):
        os.makedirs(folder_dir)

    df_mf.to_csv("{fld}\\mf_{ctry}.csv"
                .format(fld=folder_dir,
                        ctry=country_group_code),
                index=False)

    if inc_agefilter:
        folder_name = (
            "./data/mutual-funds/post-processing/{}_age-filtered/initialised"
            .format(folder_name)
        ) 
        
        if not os.path.exists(folder_dir):
            os.makedirs(folder_dir)

        df_mf.to_csv("{fld}\\mf_{ctry}.csv"
                    .format(fld=folder_dir,
                            ctry=country_group_code),
                    index=False)
    
    elapsed_time = datetime.now() - start_time
    print(f"Process {process_id} ({country_group_code}): Data saved and processed ended ({elapsed_time} passed since process start)")

def process_fund_data_wrapped(process_id):
    process_fund_data(
        COUNTRY_GROUPS[process_id], currency_type="usd", raw_ret_only=True,
        polation_method="interpolate", strict_eq=True, exc_finre=False,
        inv_targets=True, inc_agefilter=True
    )

if __name__ == "__main__":
    main_start_time = datetime.now()
    num_groups = len(COUNTRY_GROUPS)
    
    with multiprocessing.Pool(processes=4) as pool:
        pool.map(process_fund_data_wrapped, range(num_groups))

    print(f"All processes complete in {datetime.now() - main_start_time}")