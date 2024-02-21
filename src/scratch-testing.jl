using DataFrames
using CSV
using Arrow
using GLM
using Dates
using StatsBase
using Base.Threads
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function test()
    mf_filename = joinpath(DIRS.mf.refined, "mf-data.arrow")
    factors_filename = joinpath(DIRS.combo.factors, "factors.arrow")

    mf_data = loadarrow(mf_filename)
    factors_data = loadarrow(factors_filename)


end