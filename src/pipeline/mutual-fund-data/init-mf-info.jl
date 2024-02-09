using Revise
using DataFrames
using CSV
using Arrow

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function init_mf_info()
    task_start = time()
    filepath = joinpath(DIRS.mf.raw, "info.csv")
    
    mf_info = init_raw(filepath, info=true)

    printtime("initialising mutual fund info", task_start, minutes=false)
    return mf_info
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = init_mf_info()
    output_filename = makepath(DIRS.mf.init, "mf-info.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing mutual fund info", task_start, minutes=false)
end