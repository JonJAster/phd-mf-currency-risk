module DataPrep

export
    initialise_data,
    prepare_flow_controls

using
    DataFrames,
    Arrow,
    CSV,
    Base.Threads,
    Dates

include("CommonConstants.jl")
include("CommonFunctions.jl")
using
    .CommonFunctions,
    .CommonConstants

const INPUT_DIR_MF = joinpath(DIRS.fund, "post-processing")
const INPUT_DIR_FX = joinpath(DIRS.currency, "factor-series")
const INPUT_DIR_EQ = joinpath(DIRS.equity, "factor-series")

const READ_COLUMNS_MF = [:fundid, :date, :ret_gross_m, :domicile]

function initialise_data(options_folder)
    println("Reading data...")
    filename_mf = joinpath(INPUT_DIR_MF, options_folder, "main/fund_data.arrow")
    filename_fx = joinpath(INPUT_DIR_FX, "currency_factors.arrow")
    filename_longshort = joinpath(INPUT_DIR_EQ, "equity_factors.arrow")
    filename_market = joinpath(INPUT_DIR_EQ, "global_mkt_data.arrow")

    fund_data = Arrow.Table(filename_mf) |> DataFrame
    currency_factors = Arrow.Table(filename_fx) |> DataFrame
    longshort_factors = Arrow.Table(filename_longshort) |> DataFrame
    market_data = Arrow.Table(filename_market) |> DataFrame
    
    partialjoin = innerjoin(fund_data, longshort_factors, currency_factors, on=:date)
    main_data = innerjoin(
        partialjoin, market_data, on=[:cur_code, :date], matchmissing=:notequal
    )

    return main_data
end

function prepare_flow_controls(data)
    
end

function rolling_std(data, col, window)
    data[!, :std] = Vector{Union{Missing, Float64}}(missing, size(data, 1))

    for i in 1:size(data, 1)
        i < window && continue
        window_start = i - window + 1
        window_end = i

        data[window_start, :fundid] != data[window_end, :fundid] && continue

        start_date = Dates.lastdayofmonth((data[window_end, :date] - Month(window-1)))
        data[window_start, :date] != start_date && continue
        data[i, :std] = std(data[window_start:window_end, col])
    end
end

end