module DataReader

export initialise_main_data

using
    DataFrames,
    Arrow,
    CSV,
    Base.Threads,
    Dates

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using
    .CommonFunctions,
    .CommonConstants

const INPUT_DIR_MF = joinpath(DIRS.fund, "post-processing")
const INPUT_DIR_FX = joinpath(DIRS.currency, "factor-series")
const INPUT_DIR_EQ = joinpath(DIRS.equity, "factor-series")

const READ_COLUMNS_MF = [:fundid, :date, :ret_gross_m, :domicile]

function initialise_data(options_folder=option_foldername(; DEFAULT_OPTIONS...))
    println("Reading data...")
    filename_mf = joinpath(INPUT_DIR_MF, options_folder, "main/fund_data.arrow")
    filename_fx = joinpath(INPUT_DIR_FX, "currency_factors.arrow")
    filename_longshort = joinpath(INPUT_DIR_EQ, "equity_factors.arrow")
    filename_market = joinpath(INPUT_DIR_EQ, "global_mkt_data.arrow")

    fund_data = Arrow.Table(filename_mf) |> DataFrame
    currency_factors = Arrow.Table(filename_fx) |> DataFrame
    longshort_factors = Arrow.Table(filename_longshort) |> DataFrame
    market_data = Arrow.Table(filename_market) |> DataFrame
    
    println("Merging data...")
    partialjoin = innerjoin(fund_data, longshort_factors, currency_factors, on=:date)
    main_data = innerjoin(
        partialjoin, market_data, on=[:cur_code, :date], matchmissing=:notequal
    )

    return main_data
end

end