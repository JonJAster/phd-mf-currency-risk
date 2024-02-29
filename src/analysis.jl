using Revise
using DataFrames
using CSV
using Arrow
using Dates
using StatsBase
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function main()
    flow_betas = Dict{String, DataFrame}()
    for model in MODELS
        model_name = model[1]
        flow_beta_filename = joinpath(DIRS.combo.flow_betas, "$model_name.arrow")
        flow_betas[model_name] = loadarrow(flow_beta_filename)
    end

    for (k,v) in flow_betas
        println(k)
        println(v)
        println()
    end
end

main()