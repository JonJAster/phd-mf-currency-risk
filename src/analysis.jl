using Revise
using DataFrames
using CSV
using Arrow
using GLM
using Dates
using StatsBase
using Base.Threads
using LinearAlgebra
using Plots
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function analysis()
    for model_name in keys(MODELS)
        println("Flow betas for $model_name")
        flow_betas_fn = joinpath(DIRS.combo.flow_betas, "$model_name.arrow")
        flow_betas = loadarrow(flow_betas_fn)
        println(flow_betas)
        println()
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    analysis()
end