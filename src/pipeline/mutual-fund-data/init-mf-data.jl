using Revise
using DataFrames
using CSV
using Arrow
using Dates

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function init_mf_data()
    task_start = time()
    mf_data_collection = _read_mf_data()
    mf_data = reduce(
        (l,r)->outerjoin(l,r; on=[:fundid, :secid, :date]),
        mf_data_collection
    )

    drop_allmissing!(mf_data, Not([:fundid, :secid, :date]); dims=:rows)

    printtime("initialising mutual fund data", task_start, minutes=false)
    return mf_data
end

function _read_mf_data()
    folder = DIRS.mf.raw
    files = readdir(folder, )
    data = DataFrame[]
    for file in files
        file == "info.csv" && continue

        filepath = joinpath(folder, file)
        fieldname = splitext(file)[1]
        
        data_part = init_raw(filepath)
        data_part_melt = stack(
            data_part, Not([:name, :fundid, :secid]), [:fundid, :secid],
            variable_name=:date, value_name=fieldname
        )

        data_part_melt.date = Date.(data_part_melt.date)

        push!(data, data_part_melt)
    end
    return data
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = init_mf_data()
    output_filename = makepath(DIRS.mf.init, "mf-data.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing mutual fund data", task_start, minutes=false)
end