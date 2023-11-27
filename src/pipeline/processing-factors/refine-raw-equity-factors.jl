using DataFrames
using CSV
using Arrow
using Dates

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using .CommonFunctions
using .CommonConstants

const INPUT_DIR = joinpath(DIRS.equity, "raw/factors")
const INPUT_DIR_RISKFREE = joinpath(DIRS.equity, "raw")
const OUTPUT_DIR = joinpath(DIRS.equity, "factor-series")

const GLOBAL_FACTOR_LIST = ["smb", "hml", "cma", "rmw", "wml"]
const GLOBAL_READ_COLUMNS = [:date, :ret]
const USA_FACTOR_LIST = ["mkt", "smb", "hml", "wml"]

const GLOBAL_DATEFORMAT = DateFormat("yyyy-mm-dd")
const USA_DATEFORMAT = DateFormat("dd/mm/yyyy")

function main()
    time_start = time()

    println("Reading data...")
    global_factors_list = read_global_factors_data()

    global_mkt_filename = joinpath(INPUT_DIR, "global_mkt.csv")
    global_mkt = CSV.read(global_mkt_filename, DataFrame, dateformat=USA_DATEFORMAT)

    usa_filename = joinpath(INPUT_DIR, "usa_factors.csv")
    usa_factors = CSV.read(usa_filename, DataFrame, dateformat=USA_DATEFORMAT)

    println("Combining data...")
    complete_global_list = [global_factors_list..., global_mkt]
    global_factors = reduce((x,y)->innerjoin(x, y, on=:date), complete_global_list)
    
    output_filestring_global = makepath(OUTPUT_DIR, "global_equity_factors.arrow")
    output_filestring_usa = makepath(OUTPUT_DIR, "usa_equity_factors.arrow")

    println("Writing data...")
    Arrow.write(output_filestring_global, global_factors)
    Arrow.write(output_filestring_usa, usa_factors)

    time_duration = round(time() - time_start, digits=2)
    println("Finished combining equity factors in $time_duration seconds")
end

function read_global_factors_data()
    factors_list = []

    for factor_code in GLOBAL_FACTOR_LIST
        filestring = joinpath(INPUT_DIR, "global_$factor_code.csv")
        factor_data = CSV.read(
            filestring, DataFrame, select=GLOBAL_READ_COLUMNS, dateformat=GLOBAL_DATEFORMAT
        )
        factor_data.ret .*= 100
        rename!(factor_data, :ret => Symbol(factor_code))
        push!(factors_list, factor_data)
    end

    return factors_list
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end