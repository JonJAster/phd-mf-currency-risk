using FromFile
using DataFrames
using Arrow
using CSV
using BenchmarkTools
using DataStructures
using Dates
using GLM
using Plots
using StatsBase

@from "../utils.jl" using ProjectUtilities
includet("../pipeline/processing-funds/aggregate-mf-info.jl")

if false
    data1 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-1.csv")
    data12 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-12.csv")
    data13 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-13.csv")

    fundsec_info = qload(PATHS.groupedfunds, "info")
    df = copy(fundsec_info)
    @enter dropmissing!(df, :fundid)
    @enter testdropmissing!(df, :fundid)
    @benchmark dropmissing!(df, :fundid, disallowmissing=false) setup=(df = copy(fundsec_info)) evals=1
    @benchmark testdropmissing!(df, :fundid) setup=(df = copy(fundsec_info)) evals=1
    @assert isequal(dropmissing!(copy(fundsec_info), :fundid), testdropmissing!(copy(fundsec_info), :fundid))
end