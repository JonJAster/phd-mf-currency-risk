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
# using Plots
using Distributions
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function test()
    filename = joinpath(DIRS.combo.decomposed, "ff_usa_ffc4.arrow")
    betas = loadarrow(filename)
    testdf = DataFrame(a = repeat([1,2], 5), b = 1:10, c = 1:10)
    x = combine(
        groupby(testdf, :a),
        :b => sum => :bsum,
        [:b, :c] => ((x,y)->x+y) => :bplusc,
        [:b, :c] => ((x,y)->[mean(x), mean(y)])# .=> [:bmu, :cmu])
        #:c => (x->sum(x), x->mean(x)) => [:csum, :cmean]
    )
end