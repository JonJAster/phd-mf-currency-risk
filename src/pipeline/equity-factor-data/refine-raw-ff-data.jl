using Revise
using DataFrames
using Arrow
using CSV
using Dates

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function refine_raw_ff_data()
    task_start = time()

    ff_usa = _read_ff_raw("ff-usa.csv"; read_start=4, read_end=731)
    ff_usa_wml = _read_ff_raw("ff-usa-wml.csv"; read_start=14, read_end=1179)
    ff_dev = _read_ff_raw("ff-dev.csv"; read_start=7, read_end=411)
    ff_dev_wml = _read_ff_raw("ff-dev-wml.csv"; read_start=7, read_end=407)

    raw_data = CSV.read(joinpath(DIRS.eq.raw, "ff-raw.csv"), DataFrame; dateformat="yyyy-mm-dd")
    _init_ff_data!(raw_data)

    ff_data = _filter_to_desired_factors(raw_data, EQUITY_FF_FACTORS)

    printtime("refining raw FF data", task_start, minutes=false)
    return ff_data
end

function _read_ff_raw(filename; read_start, read_end)
    filepath = joinpath(DIRS.eq.raw, filename)

    file_height = countlines(filepath)
    footerskip = file_height - read_end

    raw_data = CSV.File(
        filepath;
        header=read_start,
        footerskip=footerskip,
        dateformat="yyyymm",
        types=Dict(:Column1=>Date)
    ) |> DataFrame

    series_names = names(raw_data)[2:end] |> _normalise_names .|> Symbol
    new_names = [:date; series_names]

    rename!(raw_data, new_names)

    raw_data[!, Not(:date)] ./= 100
end

function _normalise_names(names)
    relabel_excess(name) = replace(name, "-rf" => "_exc")
    normal_names = names .|> lowercase .|> relabel_excess
    return normal_names
end

if abspath(PROGRAM_FILE) == @__FILE__
    refine_raw_ff_data()
end