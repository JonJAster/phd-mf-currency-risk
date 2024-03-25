using Revise
using DataFrames
using Arrow
using StatsBase

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function replicate_bho_summary_table()
    mf_filename = joinpath(DIRS.mf.refined, "mf-data.arrow")
    info_filename = joinpath(DIRS.mf.refined, "mf-info.arrow")
    return_beta_filename = joinpath(DIRS.combo.return_betas, "usa_ff3.arrow")
    weighted_decomp_filename = joinpath(DIRS.combo.weighted, "usa_ff3.arrow")

    mf_data = loadarrow(mf_filename)
    info = loadarrow(info_filename)
    return_betas = loadarrow(return_beta_filename)
    weighted_decomp = loadarrow(weighted_decomp_filename)

    initialise_flow_data
end

if abspath(PROGRAM_FILE) == @__FILE__
    replicate_bho_summary_table()
end