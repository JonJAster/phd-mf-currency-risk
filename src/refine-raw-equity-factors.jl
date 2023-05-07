using
    DataFrames,
    CSV,
    Arrow

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using
    .CommonFunctions,
    .CommonConstants

const INPUT_DIR = joinpath(DIRS.equity, "raw/factors")
const OUTPUT_DIR = joinpath(DIRS.equity, "factor-series")

const FACTOR_LIST = ["SMB", "HML", "CMA", "RMW", "WML"]
const READ_COLUMNS = [:date, :ret]

function main()
    time_start = time()

    println("Reading data...")
    factors_list = read_factors_data()

    println("Combining data...")
    combined_factors = reduce((x,y)->innerjoin(x, y, on=:date), factors_list)
    
    output_filestring = makepath(OUTPUT_DIR, "equity_factors.arrow")

    Arrow.write(output_filestring, combined_factors)

    time_duration = round(time() - time_start, digits=2)
    println("Finished combining equity factors in $time_duration seconds")
end

function read_factors_data()
    factors_list = []

    for factor_code in FACTOR_LIST
        filestring = joinpath(INPUT_DIR, "global_$factor_code.csv")
        factor_data = CSV.read(filestring, DataFrame, select=READ_COLUMNS)
        rename!(factor_data, :ret => Symbol(factor_code))
        push!(factors_list, factor_data)
    end

    return factors_list
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end