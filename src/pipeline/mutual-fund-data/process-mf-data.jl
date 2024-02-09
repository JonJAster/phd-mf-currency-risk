using Revise
using DataFrames
using Arrow

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function process_mf_data()
    task_start = time()
    return  
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = process_mf_data()
    output_filename = makepath(DIRS.mf.refined, "mf-data.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing refined mutual fund data", task_start, minutes=false)
end