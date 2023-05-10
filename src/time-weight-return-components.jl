using
    DataFrames,
    Arrow,
    Dates,
    Base.Threads

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")

using
    .CommonConstants,
    .CommonFunctions

const INPUT_DIR = joinpath(DIRS.fund, "post-processing")
const OUTPUT_DIR = INPUT_DIR

const NONFACTOR_COLS = [:fundid, :date]

const ReturnColumns = Matrix{Union{Missing, Float64}}

function main(options_folder=option_foldername(; DEFAULT_OPTIONS...))
    time_start = time()

    savelock = ReentrantLock()
    @threads for model in COMPLETE_MODELS
        model_name = name_model(model)
        model_filename = joinpath(
            INPUT_DIR, options_folder, "decompositions", "$model_name.arrow"
        )

        model_returns = Arrow.Table(model_filename) |> DataFrame
        select!(model_returns, Not(:ret))

        weighted_returns = (
            timeweight_returns(model_returns, DEFAULT_DECAY, DEFAULT_TIMEWEIGHT_LAGS)
        )

        lock(savelock) do
            model_output_filename = makepath(
                OUTPUT_DIR, options_folder, "weighted-decompositions", "$model_name.arrow"
            )
            
            Arrow.write(model_output_filename, weighted_returns)
        end
    end

    time_duration_s = round(time() - time_start, digits=2)
    time_duration_m = round(time_duration_s/60, digits=2)
    println(
        "Finished time-weighting fund return components in $time_duration_s seconds " *
        "($time_duration_m minutes)"
    )
end

function timeweight_returns(model_returns, decay_rate, lags)
    factor_return_cols = setdiff(propertynames(model_returns), NONFACTOR_COLS)
    n_obs = size(model_returns, 1)
    n_factors = length(factor_return_cols)
    
    weighted_returns = copy(model_returns)
    weighted_returns[!, factor_return_cols] = ReturnColumns(missing, n_obs, n_factors)
    
    for i in 1:size(model_returns, 1)
        i <= lags && continue
        window_fundid = model_returns[i, :fundid]
        window_enddate = model_returns[i, :date]
        window_startdate = window_enddate - Dates.Month(lags)

        extract_start = i-lags
        extract_end = i

        model_returns[extract_start, :fundid] != window_fundid && continue
        date_extract = model_returns[extract_start:extract_end, :date]
        date_start_offset = findfirst(>=(window_startdate), date_extract)

        window_start = extract_start + date_start_offset - 1
        window_end = i

        for factor in factor_return_cols
            window_returns = model_returns[window_start:window_end, factor]
            weighted_returns[i, factor] = decay_weighted(window_returns, decay_rate)
        end
    end

    dropmissing!(weighted_returns, factor_return_cols)

    return weighted_returns
end

function decay_weighted(returns, decay_rate)
    window_size = length(returns)
    decay_factor = ℯ .^ (-decay_rate .* (window_size-1:-1:0))
    decayed_returns = returns .* decay_factor

    count(!ismissing, decayed_returns) == 0 && return missing

    time_weighted_return = sum(skipmissing(decayed_returns))
    return time_weighted_return
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end

returns = [0.23, 0.12, -0.02, .11, .02, -.03]
        