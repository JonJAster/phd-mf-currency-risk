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

    options_folder=option_foldername(; DEFAULT_OPTIONS...)
    path = joinpath(DIRS.fund, "post-processing", options_folder, "main/fund_data.arrow")
    path_factors = joinpath(DIRS.equity, "factor-series/equity_factors.arrow")
    path_curr = joinpath(DIRS.currency, "factor-series/currency_factors.arrow")

    df = Arrow.Table(path) |> DataFrame
    df_factors = Arrow.Table(path_factors) |> DataFrame
    df_curr = Arrow.Table(path_curr) |> DataFrame

    minimum(df.date)
end