using Revise
using BenchmarkTools
using DataFrames
using CSV
using Arrow
using GLM
using Dates
using DataStructures
using StatsBase
using Base.Threads
using LinearAlgebra
using Plots
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function test()
    factors_data = loadarrow(joinpath(DIRS.combo.factors, "factors.arrow"))
    mf_data = loadarrow(joinpath(DIRS.mf.refined, "mf-excess-returns.arrow"))

    ff_factors = factors_data[startswith.(factors_data.source_id, "ff_"), :]
    jkp_factors = factors_data[startswith.(factors_data.source_id, "jkp_"), :]

    compare_mkt = innerjoin(
        ff_factors[ff_factors.factor .== "mkt", Not(:source_id, :factor)],
        jkp_factors[jkp_factors.factor .== "mkt", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )
end