using Arrow
using CSV
using DataFrames
using Dates
using StatsBase

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
include("shared/DataInit.jl")
using .CommonFunctions
using .CommonConstants
using .DataInit

function test()


    path = joinpath(DIRS.fund, "domicile-grouped")

    df = CSV.read(joinpath(path, "local-monthly-gross-returns/mf_local-monthly-gross-returns_usa.csv"), DataFrame)

    melted_df = stack(df, Not(["name", "fundid", "secid"]), ["name", "fundid", "secid"])

    nobs = size(melted_df, 1)
    nfunds = length(unique(melted_df.fundid))

    options_folder=option_foldername(; DEFAULT_OPTIONS...)

    df_usa = initialise_flow_data(options_folder, COMPLETE_MODELS[1]; ret=:weighted)
end

function rolling_std(data, col, window; lagged)
    rolling_stdx = Vector{Union{Missing, Float64}}(missing, size(data, 1))

    i=216

    for i in 1:size(data, 1)
        i < window && continue
        window_start = i - window + 1
        lagged ? window_end = i - 1 : window_end = i

        data[window_start, :fundid] != data[window_end, :fundid] && continue

        start_date = Dates.lastdayofmonth((data[window_end, :date] - Month(window-2)))
        data[window_start, :date] != start_date && continue
        rolling_stdx[i] = std(data[window_start:window_end, col])
    end

    return rolling_stdx
end