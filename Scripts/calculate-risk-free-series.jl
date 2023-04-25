using DataFrames
using CSV

include("CommonFunctions.jl")
using .CommonFunctions

const INPUT_FILESTRING_RF = "./data/raw/equities/usd_riskfree.csv"
const INPUT_FILESTRING_FX = "./data/prepared/currencies/currency_rates.csv"
const OUTPUT_FILESTRING_BASE = "./data/transformed/equities"
const READ_COLUMNS_FX = [:cur_code, :date, :spot_mid, :forward_mid]

function build_local_rf(usd_rf, rates_data)
    currency_list = unique(rates_data.cur_code)

    local_riskfree_set = DataFrame[]
    push_with_currency_code!(local_riskfree_set, usd_rf, "USD", :rf)

    for currency in currency_list
        currency_riskfree = calculate_riskfree_by_cip(usd_rf, rates_data, currency)
        push_with_currency_code!(local_riskfree_set, currency_riskfree, currency, :rf)
    end

    output = vcat(local_riskfree_set...)
    sort!(output, [:cur_code, :date])
    dropmissing!(output, :rf)
    return output
end

function calculate_riskfree_by_cip(base_rf, rates_data, currency_code)
    country_rates = rates_data[rates_data.cur_code .== currency_code, :]

    country_riskfree = innerjoin(country_rates, base_rf, on=:date)
    country_riskfree.rf = (
        country_riskfree.rf ./ country_riskfree.spot_mid .* country_riskfree.forward_mid
    )

    return country_riskfree
end

function main()
    time_start = time()
    println("Reading data...")
    usd_rf = CSV.read(INPUT_FILESTRING_RF, DataFrame)
    fx_rates = CSV.read(INPUT_FILESTRING_FX, DataFrame, select=READ_COLUMNS_FX)

    println("Building local risk-free series...")
    local_rf = build_local_rf(usd_rf, fx_rates)

    if !isdir(OUTPUT_FILESTRING_BASE)
        mkpath(OUTPUT_FILESTRING_BASE)
    end

    output_filestring = joinpath(OUTPUT_FILESTRING_BASE, "riskfree.csv")

    CSV.write(output_filestring, local_rf)

    time_duration = round(time() - time_start, digits=2)
    println("Finished building local risk-free rates in $time_duration seconds")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end