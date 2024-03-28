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
    filepaths = readdir(DIRS.mf.raw, join=true)

    info = CSV.read(joinpath(DIRS.mf.raw, "info.csv"), DataFrame)

    info_fundids = info.FundId |> Set
    info_secids = info.SecId |> Set

    data = loadarrow(joinpath(DIRS.mf.init, "mf-data.arrow"))

        data_fundids = union(data_fundids, file_fundids)
        data_secids = union(data_secids, file_secids)
    end

    gret_data = CSV.read(joinpath(DIRS.mf.raw, "gross_returns.csv"), DataFrame)
    println(count(!ismissing, gret_data[:, 4:end]|>Matrix))
    21330*410
end