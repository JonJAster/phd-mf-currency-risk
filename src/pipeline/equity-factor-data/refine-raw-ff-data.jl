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

    ff_usa.source_id .= "ff_usa"
    ff_usa_wml.source_id .= "ff_usa"
    ff_dev.source_id .= "ff_dev"
    ff_dev_wml.source_id .= "ff_dev"

    ff_usa_long = stack(
        ff_usa, Not([:date, :source_id, :rf]), [:date, :source_id];
        variable_name=:factor, value_name=:ret
    )
    ff_usa_wml_long = stack(
        ff_usa_wml, Not([:date, :source_id]);
        variable_name=:factor, value_name=:ret
    )
    ff_dev_long = stack(
        ff_dev, Not([:date, :source_id, :rf]), [:date, :source_id];
        variable_name=:factor, value_name=:ret
    )
    ff_dev_wml_long = stack(
        ff_dev_wml, Not([:date, :source_id]);
        variable_name=:factor, value_name=:ret
    )

    ff_factors = vcat(ff_usa_long, ff_usa_wml_long, ff_dev_long, ff_dev_wml_long)
    ff_rf = ff_usa[:, [:date, :rf]]

    output = (
        factors = ff_factors,
        rf = ff_rf
    )

    printtime("refining raw FF data", task_start, minutes=false)
    return output
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
    cut_excess_label(name) = replace(name, "-rf" => "")
    fix_wml_label(name) = replace(name, "mom   " => "wml")
    normal_names = names .|> lowercase .|> cut_excess_label |> fix_wml_label
    return normal_names
end

if !isnothing(match(r"terminalserver.jl", PROGRAM_FILE))
    output_data = refine_raw_ff_data()
    output_filename_factors = makepath(DIRS.eq.factors, "ff.arrow")
    output_filename_rf = makepath(DIRS.eq.refined, "rf.arrow")

    task_start = time()
    Arrow.write(output_filename_factors, output_data.factors)
    Arrow.write(output_filename_rf, output_data.rf)
    printtime("writing refined FF data", task_start, minutes=false)
end