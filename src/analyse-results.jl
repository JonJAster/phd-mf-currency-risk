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
    old_data_new_script_filename = joinpath(DIRS.test, "old-comparison-data/new-format/magnitude-adjusted/flow-betas/dev_ff3_ver.arrow")
    comparison_flow_beta_filename = joinpath(DIRS.test, "old-comparison-data/old-format/old_world_ff3_verdelhan_flow_betas.arrow")

    flow_betas = loadarrow(flow_beta_filename)
    old_data_new_script = loadarrow(old_data_new_script_filename)
    comparison_flow_betas = loadarrow(comparison_flow_beta_filename)
end

if abspath(PROGRAM_FILE) == @__FILE__
    analyse_results()
end