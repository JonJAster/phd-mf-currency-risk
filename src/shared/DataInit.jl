module DataInit

export
    initialise_base_data,
    initialise_flow_data

using
    DataFrames,
    Arrow,
    CSV,
    Base.Threads,
    Statistics,
    Dates

include("CommonConstants.jl")
include("CommonFunctions.jl")
using
    .CommonFunctions,
    .CommonConstants

const INPUT_DIR_MF = joinpath(DIRS.fund, "post-processing")
const INPUT_DIR_MFINFO = joinpath(DIRS.fund, "info")
const INPUT_DIR_FX = joinpath(DIRS.currency, "factor-series")
const INPUT_DIR_EQ = joinpath(DIRS.equity, "factor-series")

const READ_COLUMNS_MF = [:fundid, :date, :ret_gross_m, :domicile]

function initialise_base_data(options_folder)
    filename_mf = joinpath(INPUT_DIR_MF, options_folder, "main/fund_data.arrow")
    filename_fx = joinpath(INPUT_DIR_FX, "currency_factors.arrow")
    filename_longshort = joinpath(INPUT_DIR_EQ, "equity_factors.arrow")
    filename_market = joinpath(INPUT_DIR_EQ, "unhedged_global_mkt.arrow")

    fund_data = Arrow.Table(filename_mf) |> DataFrame
    currency_factors = Arrow.Table(filename_fx) |> DataFrame
    longshort_factors = Arrow.Table(filename_longshort) |> DataFrame
    market_data = Arrow.Table(filename_market) |> DataFrame
    
    partialjoin = innerjoin(fund_data, longshort_factors, currency_factors, on=:date)
    main_data = innerjoin(
        partialjoin, market_data, on=[:currency, :date], matchmissing=:notequal
    )

    return main_data
end

function initialise_flow_data(options_folder, model; ret, us_only=true)
    model_name = name_model(model)
    
    if ret == :raw
        filename_decomposition = joinpath(
            INPUT_DIR_MF, options_folder, "decompositions/$model_name.arrow"
        )
    elseif ret == :weighted
        filename_decomposition = joinpath(
            INPUT_DIR_MF, options_folder, "weighted-decompositions/$model_name.arrow"
        )
    else
        error("ret must be :raw or :weighted")
    end
    
    filename_mf = joinpath(INPUT_DIR_MF, options_folder, "main/fund_data.arrow")
    filename_info = joinpath(INPUT_DIR_MFINFO, "mf_info.arrow")

    fund_base_data = Arrow.Table(filename_mf) |> DataFrame
    fund_info = Arrow.Table(filename_info) |> DataFrame
    decomposed_returns = Arrow.Table(filename_decomposition) |> DataFrame

    if us_only
        fund_base_data = fund_base_data[fund_base_data.domicile .== "USA", :]
    end

    fund_base_data.std_return_12m = rolling_std(fund_base_data, :ret, 12; lagged=true)

    select!(
        fund_base_data,
        [:fundid, :date, :fund_flow, :fund_assets, :mean_costs, :std_return_12m]
    )
    select!(fund_info, [:fundid, :no_load, :inception_date])

    fund_rets_data = innerjoin(fund_base_data, decomposed_returns, on=[:fundid, :date])
    fund_full_data = innerjoin(
        fund_rets_data, fund_info, on=:fundid, matchmissing=:notequal
    )

    fund_full_data.age = (
        12*(year.(fund_full_data.date) .- year.(fund_full_data."inception_date")) .+
        (month.(fund_full_data.date) .- month.(fund_full_data."inception_date")) .+ 1
    )

    output_data = fund_full_data[fund_full_data.age .>= 0, :]

    output_data.log_size = log.(output_data.fund_assets)
    output_data.log_age = log.(output_data.age)
    
    sort!(output_data, [:fundid, :date])
    select!(output_data, Not(["inception_date", "age", "fund_assets"]))

    return output_data
end

function rolling_std(data, col, window; lagged)
    rolling_std = Vector{Union{Missing, Float64}}(missing, size(data, 1))

    for i in 1:size(data, 1)
        i < window && continue
        window_start = i - window + 1
        lagged ? window_end = i - 1 : window_end = i

        data[window_start, :fundid] != data[window_end, :fundid] && continue

        start_date = Dates.lastdayofmonth((data[window_end, :date] - Month(window-2)))
        data[window_start, :date] != start_date && continue
        rolling_std[i] = std(data[window_start:window_end, col])
    end

    return rolling_std
end

end