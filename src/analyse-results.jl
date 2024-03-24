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

function analyse_results()
    flow_beta_filename = joinpath(DIRS.combo.flow_betas, "dev_ff3_ver.arrow")

    flow_betas = loadarrow(flow_beta_filename)
end

if abspath(PROGRAM_FILE) == @__FILE__
    analyse_results()
end