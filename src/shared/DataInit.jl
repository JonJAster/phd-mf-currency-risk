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

function initialise_base_data(options_folder; filtered=false, region=:world)
    filtered ? parent_dir = "filtered" : parent_dir = "main"
    filename_mf = joinpath(
        INPUT_DIR_MF, options_folder, parent_dir, "excess_fund_data.arrow"
    )

    filename_fx = joinpath(INPUT_DIR_FX, "currency_factors.arrow")
    
    if region == :world
        filename_eq = joinpath(INPUT_DIR_EQ, "global_equity_factors.arrow")
    elseif region == :usa
        filename_eq = joinpath(INPUT_DIR_EQ, "usa_equity_factors.arrow")
    else
        error("region must be :usa or :world")
    end

    fund_data = Arrow.Table(filename_mf) |> DataFrame
    currency_factors = Arrow.Table(filename_fx) |> DataFrame
    equity_factors = Arrow.Table(filename_eq) |> DataFrame
    
    main_data = innerjoin(fund_data, equity_factors, currency_factors, on=:date)

    return main_data
end

function initialise_flow_data(options_folder, model; ret)
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
    
    filename_mf = joinpath(INPUT_DIR_MF, options_folder, "main/excess_fund_data.arrow")
    filename_info = joinpath(INPUT_DIR_MFINFO, "mf_info.arrow")

    fund_base_data = Arrow.Table(filename_mf) |> DataFrame
    fund_info = Arrow.Table(filename_info) |> DataFrame
    decomposed_returns = Arrow.Table(filename_decomposition) |> DataFrame

    fund_base_data.std_return_12m = rolling_std(fund_base_data, :ex_ret, 12; lagged=true)

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