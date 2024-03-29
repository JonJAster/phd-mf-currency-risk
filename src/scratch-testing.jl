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
    raw_factor_data_fn = joinpath(DIRS.eq.raw, "region-lms.csv")
    raw_factor_data = CSV.read(raw_factor_data_fn, DataFrame, dateformat="yyyy-mm-dd")
    maximum(raw_factor_data.date)

    raw_mkt_data_fn = joinpath(DIRS.eq.raw, "country-mkt.csv")
    raw_mkt_data = CSV.read(raw_mkt_data_fn, DataFrame, dateformat="yyyy-mm-dd")
    usa_mkt = raw_mkt_data[raw_mkt_data.excntry .== "USA",:]
end