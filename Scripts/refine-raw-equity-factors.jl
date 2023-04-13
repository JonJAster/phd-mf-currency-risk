using DataFrames
using CSV

const INPUT_FILESTRING_BASE = "./data/raw/equities/factors"
const OUTPUT_FILESTRING_BASE = "./data/prepared/equities"
const FACTOR_LIST = ["SMB", "HML", "CMA", "RMW", "WML"]
const READ_COLUMNS = [:date, :ret]

function read_factors_data()
    factors_list = []

    for factor_code in FACTOR_LIST
        filestring = joinpath(INPUT_FILESTRING_BASE, "global_$factor_code.csv")
        factor_data = CSV.read(filestring, DataFrame, select=READ_COLUMNS)
        rename!(factor_data, :ret => Symbol(factor_code))
        push!(factors_list, factor_data)
    end

    return factors_list
end

function main()
    time_start = time()

    println("Reading data...")
    factors_list = read_factors_data()

    println("Combining data...")
    combined_factors = reduce((x,y)->innerjoin(x, y, on=:date), factors_list)

    if !isdir(OUTPUT_FILESTRING_BASE)
        mkpath(OUTPUT_FILESTRING_BASE)
    end

    OUTPUT_FILESTRING = joinpath(OUTPUT_FILESTRING_BASE, "equity_factors.csv")

    CSV.write(OUTPUT_FILESTRING, combined_factors)

    time_duration = round(time() - time_start, digits=2)
    println("Finished combining equity factors in $time_duration seconds")
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end