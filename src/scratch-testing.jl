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
    info = CSV.read(joinpath(DIRS.mf.raw, "info.csv"), DataFrame)

    info_fundids = info.FundId |> Set
    info_secids = info.SecId |> Set

    raw_gret = CSV.read(joinpath(DIRS.mf.raw, "gross_returns.csv"), DataFrame)

    gb = groupby(raw_gret, :FundId)
    fund_raw_gret = combine(
        gb,
        propertynames(raw_gret)[4:end] .=> (x->all(!ismissing, x)) 
    )

    count(x->x, fund_raw_gret[:, 2:end] |> Matrix)

    data = loadarrow(joinpath(DIRS.mf.init, "mf-data.arrow"))

    1-length(unique(data.fundid))/5145
    1-length(unique(data.secid))/21333
    
    gret_data = CSV.read(joinpath(DIRS.mf.raw, "gross_returns.csv"), DataFrame)
    println(count(!ismissing, gret_data[:, 4:end]|>Matrix))
    21330*410
end