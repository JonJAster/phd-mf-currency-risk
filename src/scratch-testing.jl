using Revise
using BenchmarkTools
using DataFrames
using CSV
using REPL
using Arrow
using GLM
using Dates
using DataStructures
using StatsBase
using Base.Threads
using LinearAlgebra
using StatsModels
using Plots
using Distributions
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function test()
    tw_ret_file = joinpath(DIRS.combo.weighted, "ff_usa_ffc6.arrow")
    tw_ret = loadarrow(tw_ret_file)

    tw_ret_file_dev = joinpath(DIRS.combo.weighted, "ff_dev_ffc6.arrow")
    tw_ret_dev = loadarrow(tw_ret_file_dev)

    minimum(tw_ret.date)
    minimum(tw_ret_dev.date)

    factors_file = joinpath(DIRS.eq.factors, "ff.arrow")
    factors = loadarrow(factors_file)

    factors_usa = factors[factors.source_id .== "ff_usa", :]
    factors_dev = factors[factors.source_id .== "ff_dev", :]

    println("USA ", maximum([minimum(factors_usa[factors_usa.factor .== f, :date]) for f in unique(factors_usa.factor)]), " ", minimum([maximum(factors_usa[factors_usa.factor .== f, :date]) for f in unique(factors_usa.factor)]))
    for f in unique(factors_usa.factor)
        println(f, " ", minimum(factors_usa[factors_usa.factor .== f, :date]), " ", maximum(factors_usa[factors_usa.factor .== f, :date]))
    end
    
    println()
    println("DEV ", maximum([minimum(factors_dev[factors_dev.factor .== f, :date]) for f in unique(factors_dev.factor)]), " ", minimum([maximum(factors_dev[factors_dev.factor .== f, :date]) for f in unique(factors_dev.factor)]))
    for f in unique(factors_dev.factor)
        println(f, " ", minimum(factors_dev[factors_dev.factor .== f, :date]), " ", maximum(factors_dev[factors_dev.factor .== f, :date]))
    end

    betas_file_usa = joinpath(DIRS.combo.return_betas, "ff_usa_ffc6.arrow")
    betas_usa = loadarrow(betas_file)
    be



    factors_file = joinpath(DIRS.eq.factors, "ff.arrow")
    factors = loadarrow(factors_file)

    factor_list = unique(factors.factor)
    regional_factors = Dict()
    for i in factor_list
        regional_factors[i] = unstack(
            factors[factors.factor .== i, :], :date, :source_id, :ret
        ) |> dropmissing
    end

    for i in factor_list
        println(i)
        println(cor(Matrix(regional_factors[i][:, 2:end]))[1, 2])
    end

end