using DataFrames
using CSV
using GLM

include("CommonFunctions.jl")
using .CommonFunctions

const INPUT_FILESTRING_BASE_FUNDS = "./data/transformed/mutual-funds"
const INPUT_FILESTRING_LONGSHORT_FACTORS = "./data/prepared/equities/equity_factors.csv"
const INPUT_FILESTRING_MARKET = "./data/transformed/equities/global_MKT.csv"
const INPUT_FILESTRING_CURRENCY_MAP = "./data/raw/currency_to_country.csv"
const READ_COLUMNS_FUNDS = [:fundid, :date, :ret_gross_m, :domicile]
const OUTPUT_FILESTRING_BASE = ".data/results/"

function main(options_folder)
    time_start = time()

    println("Loading data...")
    fund_data = load_fund_data(options_folder, select=READ_COLUMNS_FUNDS)
    longshort_factors = CSV.read(INPUT_FILESTRING_LONGSHORT_FACTORS, DataFrame)
    market_factor = CSV.read(INPUT_FILESTRING_MARKET, DataFrame)
    currency_map = CSV.read(INPUT_FILESTRING_CURRENCY_MAP, DataFrame)

    println("Mapping countries to currencies...")
    fund_data.cur_code = map_currency(fund_data.date, fund_data.domicile, currency_map)

    full_data = (
        innerjoin(fund_data, longshort_factors, on=:date) >| partialjoin ->
        innerjoin(partialjoin, market_factor, on=[:cur_code, :date])
    )
    
    regression_table = full_data[:,
        [:fundid, :date, :ret_gross_m, :mkt, :SMB, :HML, :RMW, :CMA, :WML]
    ]

    println("Running regressions...")
    betas = regress_timevarying_betas(
        regression_table, :fundid, :date, :ret_gross_m,
        [:mkt, :SMB, :HML, :RMW, :CMA, :WML]
    )

    if !isdir(OUTPUT_FILESTRING_BASE)
        mkpath(OUTPUT_FILESTRING_BASE)
    end

    output_filestring = joinpath(OUTPUT_FILESTRING_BASE, options_folder, "betas.csv")
    CSV.write(output_filestring, betas)

    time_duration = round(time() - time_start, digits=2)
    println("Finished fund regressions in $time_duration seconds")
end

if abspath(PROGRAM_FILE) == @__FILE__
    folderstring = option_foldername(currency_type="local")
    main(folderstring)
end