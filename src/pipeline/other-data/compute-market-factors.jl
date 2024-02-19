using Revise
using DataFrames
using CSV
using Arrow
using Dates

include("../../shared/CommonConstants.jl")
include("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function compute_market_factors()
    task_start = time()

    map_filename = joinpath(DIRS.map.refined, "msci-class.csv")
    data_filename = joinpath(DIRS.eq.raw, "country-mkt.csv")

    msci_class_map = CSV.read(map_filename, DataFrame; dateformat="d/mm/yyyy")
    mkt_data = CSV.read(
        data_filename, DataFrame;
        select=[:excntry, :eom, :me_lag1, :mkt_vw_exc], dateformat="yyyy-mm-dd"
    )

    _init_mkt_data!(mkt_data)

    mkt_usa = mkt_data[mkt_data.country_code .== "USA", :]
    _init_region_info!(mkt_usa, "USA")

    mkt_wld = _weight_returns(mkt_data)
    _init_region_info!(mkt_wld, "WLD")

    msci_classified_data = innerjoin(mkt_data, msci_class_map, on=[:country_code, :date])
    emg_data = msci_classified_data[msci_classified_data.msci_class .== "EMG", :]
    dev_data = msci_classified_data[msci_classified_data.msci_class .== "DEV", :]

    mkt_emg = _weight_returns(emg_data)
    _init_region_info!(mkt_emg, "EMG")

    mkt_dev = _weight_returns(dev_data)
    _init_region_info!(mkt_dev, "DEV")

    mkt_factors = reduce(vcat, [mkt_usa, mkt_wld, mkt_emg, mkt_dev])

    printtime("computing market factors", task_start, minutes=false)
    return mkt_factors
end

function _init_mkt_data!(mkt_data)
    rename!(
        mkt_data,
        :excntry => :country_code,
        :eom => :date,
        :mkt_vw_exc => :mkt_exc
    )

    mkt_data.date = firstdayofmonth.(mkt_data.date)
    return mkt_data
end

function _init_region_info!(region_data, region)
    region_data.region .= region
    select!(region_data, [:region, :date, :mkt_exc])
    sort!(region_data, :date)
    return region_data
end

function _weight_returns(data)
    me_totals = combine(
        groupby(data, :date),
        :me_lag1 => sum => :me_total
    )
    me_totals[me_totals.date .== Date(2022,10,1),:]
    data_total = innerjoin(data, me_totals, on=:date)

    data_total.weighted_return = data_total.mkt_exc .* data_total.me_lag1 ./ data_total.me_total

    weighted_returns = combine(
        groupby(data_total, :date),
        :weighted_return => sum => :mkt_exc
    )
    return weighted_returns
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = compute_market_factors()
    output_filestring = makepath(DIRS.eq.refined, "mkt.arrow")

    task_start = time()
    Arrow.write(output_filestring, output_data)
    printtime("writing market factors", task_start, minutes=false)
end