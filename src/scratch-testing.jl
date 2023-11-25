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
    path_factors = joinpath(DIRS.equity, "factor-series/global_equity_factors.arrow")
    path_curr = joinpath(DIRS.currency, "factor-series/currency_factors.arrow")

    df_raw = load_raw("domicile-grouped", "usa")

    describe(df_raw)
    dropmissing!(df_raw, :ret_gross_m)

    df = Arrow.Table(path) |> DataFrame
    df_factors = Arrow.Table(path_factors) |> DataFrame
    df_curr = Arrow.Table(path_curr) |> DataFrame

    minimum(df.date)

    test_df = DataFrame(
        a = [1, missing, missing, missing, missing, missing, missing],
        b = [1, missing, missing, missing, missing, missing, 7],
        c = [1, missing, missing, missing, missing, 6, missing],
        d = [1, missing, missing, missing, 5, 6, 7],
        e = [1, missing, missing, 4, 5, missing, missing],
        f = [1, missing, 3, 4, 5, missing, 7],
        g = [1, missing, 3, 4, missing, 6, missing],
        h = [1, missing, 3, missing, missing, missing, 7],
        i = [1, missing, 3, 3, missing, missing, missing],
        k = [1, missing, 3, missing, 5, 6, 7]
    )

    dropmissing(test_df, :k)

    Matrix(test_df) .|> ismissing .|> m->any(m,1)
end