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
    refined_mf = loadarrow(joinpath(DIRS.mf.refined, "mf-data.arrow"))

    refined_mf = transform(
        groupby(refined_mf, :fundid),
        :ex_ret => (x->1:length(x)) => :cumcount
    )

    mature_mf_old = refined_mf[refined_mf.cumcount .>= 60,:]
    mature_mf = refined_mf[refined_mf.cumcount .> 60,:]

    drange(mature_mf_old)
    drange(mature_mf)

    countobs(mature_mf_old, :ex_ret)
    countobs(mature_mf, :ex_ret)
    drange(mature_mf)
    ###

    ret_betas = loadarrow(joinpath(DIRS.combo.return_betas, "dev_ff3_ver.arrow"))
    ret_betas_wide = unstack(ret_betas, [:fundid, :date], :factor, :coef)

    select!(ret_betas_wide, Not(:const))
    dropmissing!(ret_betas_wide)

    full_data = innerjoin(refined_mf, ret_betas_wide, on=[:fundid, :date])

    test1 = mature_mf[mature_mf.fundid .== "FS00008KNP", :]
    test2 = full_data[full_data.fundid .== "FS00008KNP", :]

    drange(test1)
    drange(test2)

    x = countobs(full_data, :ex_ret)
    drange(full_data)
    att(x[1], 2019)
    att(x[2], 255997)

end