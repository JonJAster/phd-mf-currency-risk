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