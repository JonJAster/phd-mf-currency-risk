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

    data_fundids = Set{String}()
    data_secids = Set{String}()
    for file in filepaths # file = filepaths[2]
        occursin("info", file) && continue
        file_data = CSV.read(file, DataFrame)
        file_fundids = file_data.FundId |> Set
        file_secids = file_data.SecId |> Set

        if !isempty(data_fundids)
            difference = setdiff(file_fundids, data_fundids)
            if !isempty(difference)
                println("fundids don't match")
                println(file)
                println(difference)
                println()
            end
        end
        if !isempty(data_secids)
            difference = setdiff(file_secids, data_secids)
            if !isempty(difference)
                println("secids don't match")
                println(file)
                println(difference)
                println()
            end
        end

        data_fundids = union(data_fundids, file_fundids)
        data_secids = union(data_secids, file_secids)
    end

    gret_data = CSV.read(joinpath(DIRS.mf.raw, "gross_returns.csv"), DataFrame)
    println(size(gret_data[4:end, 1:end]|>Matrix))
    21330*410
end