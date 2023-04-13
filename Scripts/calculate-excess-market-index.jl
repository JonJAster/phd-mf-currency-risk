using DataFrames
using CSV

const INPUT_FILESTRING_MARKET = "./data/transformed/equities/global_MKT_gross.csv"
const INPUT_FILESTRING_RF = "./data/transformed/equities/riskfree.csv"
const OUTPUT_FILESTRING_BASE = "./data/transformed/equities"

function main()
    time_start = time()

    println("Reading data...")
    market_data = CSV.read(INPUT_FILESTRING_MARKET, DataFrame)
    rf_data = CSV.read(INPUT_FILESTRING_RF, DataFrame)

    println("Calculating excess market returns...")
    excess_market = innerjoin(market_data, rf_data, on=[:cur_code, :date])
    excess_market.mkt = excess_market.mkt .- excess_market.rf

    if !isdir(OUTPUT_FILESTRING_BASE)
        mkpath(OUTPUT_FILESTRING_BASE)
    end

    output_filestring = joinpath(OUTPUT_FILESTRING_BASE, "global_MKT_excess.csv")
    CSV.write(output_filestring, excess_market)

    time_duration = round(time() - time_start, digits=2)
    println("Finished calculating excess market returns in $time_duration seconds")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end