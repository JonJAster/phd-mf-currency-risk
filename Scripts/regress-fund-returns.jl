using DataFrames
using CSV
using GLM
using Dates

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
    market_factor = CSV.read(INPUT_FILESTRING_MARKET, DataFrame, dateformat="dd/mm/yyyy")
    currency_map = CSV.read(INPUT_FILESTRING_CURRENCY_MAP, DataFrame)

    fund_data.date = Dates.lastdayofmonth.(fund_data.date)

    println("Mapping countries to currencies...")
    fund_data.cur_code = map_currency(fund_data.date, fund_data.domicile, currency_map)

    full_data = (
        innerjoin(fund_data, longshort_factors, on=:date) |> partialjoin ->
        innerjoin(partialjoin, market_factor, on=[:cur_code, :date], matchmissing=:notequal)
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

function load_fund_data(options_folder; select)
    output_data = DataFrame[]
    folderstring = joinpath(INPUT_FILESTRING_BASE_FUNDS, options_folder)

    for file in readdir(folderstring)
        filestring = joinpath(folderstring, file)
        file_data = CSV.read(filestring, DataFrame, select=select)
        push!(output_data, file_data)
    end

    return vcat(output_data...)
end

function map_currency(date_series, country_series, currency_map)
    currency_series = Vector{Union{Missing, String}}(fill(missing, length(date_series)))

    for i in 1:length(date_series)
        date = date_series[i]
        country = country_series[i]

        country_match = currency_map[!, :country_code] .== country

        date_match = not_outside.(Ref(date), currency_map.start_date, currency_map.end_date)

        matching_rows = currency_map[country_match .& date_match, :]

        nrow(matching_rows) > 1 && println("Warning: multiple match for $country on $date")

        if nrow(matching_rows) > 0
            currency_series[i] = matching_rows[1, :currency_code]
        end
    end

    return currency_series
end

function not_outside(date, start_date, end_date)
    !ismissing(start_date) && date < start_date && return false
    !ismissing(end_date) && date > end_date && return false
    return true
end

if abspath(PROGRAM_FILE) == @__FILE__
    options_folder = option_foldername(currency_type="local")
    main(options_folder)
end