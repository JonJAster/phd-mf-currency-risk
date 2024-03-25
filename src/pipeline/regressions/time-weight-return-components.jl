using Revise
using DataFrames
using Arrow
using Dates

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function time_weight_return_components(model_name)
    task_start = time()
    model_returns_filename = joinpath(DIRS.combo.decomposed, "$model_name.arrow")

    model_returns = loadarrow(model_returns_filename)
    select!(model_returns, Not(:ex_ret))

    weighted_returns = _timeweight_returns(model_returns)

    printtime("time-weighting decomposed returns for $model_name", task_start)
    return weighted_returns
end

function _timeweight_returns(model_returns)
    nonfactor_cols = [:fundid, :date]
    factor_return_cols = setdiff(propertynames(model_returns), nonfactor_cols)
    n_obs = nrow(model_returns)
    n_factors = length(factor_return_cols)
    
    weighted_returns = copy(model_returns)
    weighted_returns[!, factor_return_cols] = (
        Matrix{Union{Missing, Float64}}(missing, n_obs, n_factors)
    )
    
    for i in 1:nrow(model_returns)
        i <= TIMEWEIGHT_LAGS && continue
        window_fundid = model_returns[i, :fundid]
        window_enddate = model_returns[i, :date] - Dates.Month(1)
        window_startdate = window_enddate - Dates.Month(TIMEWEIGHT_LAGS)

        extract_start = i-TIMEWEIGHT_LAGS
        extract_end = i-1

        model_returns[extract_start, :fundid] != window_fundid && continue
        date_extract = model_returns[extract_start:extract_end, :date]
        date_start_offset = findfirst(>=(window_startdate), date_extract)

        isnothing(date_start_offset) && continue

        window_start = extract_start + date_start_offset - 1
        window_end = extract_end

        for factor in factor_return_cols
            window_returns = model_returns[window_start:window_end, factor]
            weighted_returns[i, factor] = _decay_weighted(window_returns)
        end
    end

    dropmissing!(weighted_returns, factor_return_cols)

    return weighted_returns
end

function _decay_weighted(window_returns)
    window_size = length(window_returns)
    decay_factors = ℯ .^ (-DEFAULT_DECAY .* (window_size-1:-1:0))
    weights = decay_factors ./ sum(decay_factors)
    weighted_return_contributions = window_returns .* weights

    count(!ismissing, weighted_return_contributions) == 0 && return missing

    time_weighted_return = sum(skipmissing(weighted_return_contributions))
    return time_weighted_return
end

function main()
    for model_name in keys(MODELS)
        output_data = time_weight_return_components(model_name)
        output_filename = makepath(DIRS.combo.weighted, "$model_name.arrow")

        Arrow.write(output_filename, output_data)
    end
    return
end

if abspath(PROGRAM_FILE) == @__FILE__
    task_start = time()
    main()
    printtime("time-weighting all decomposed returns", task_start)
end