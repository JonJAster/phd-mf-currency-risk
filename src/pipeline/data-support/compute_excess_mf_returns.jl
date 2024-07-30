using Revise
using DataFrames
using Arrow

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function compute_excess_mf_returns()
    task_start = time()

    mf_data_filename = joinpath(DIRS.mf.refined, "mf-simple-returns.arrow")
    rf_filename = joinpath(DIRS.eq.refined, "rf.arrow")

    mf_data = loadarrow(mf_data_filename)
    rf_data = loadarrow(rf_filename)

    mf_data = innerjoin(mf_data, rf_data, on=:date)
    mf_data.ex_ret = mf_data.ret - mf_data.rf

    select!(mf_data, Not(:ret, :rf))

    printtime("computing excess mutual fund returns", task_start, minutes=false)

    return mf_data
end

if isnothing(match(r"terminalserver.jl$", PROGRAM_FILE))
    output_data = compute_excess_mf_returns()
    output_filename = joinpath(DIRS.mf.refined, "mf-excess-returns.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing excess mutual fund returns", task_start, minutes=false)
end