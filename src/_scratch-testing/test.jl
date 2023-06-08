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

if false
    data = qload(PATHS.rawfunds, FUND_FIELDS.cost)
    @time data1 = qload(PATHS.rawfunds, FUND_FIELDS.cat, "part-1.csv")
    @time data2 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-1.csv")
    data2 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-2.csv")
    data3 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-3.csv")
    data4 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-4.csv")
    data5 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-5.csv")
    data6 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-6.csv")
    data7 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-7.csv")
    data8 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-8.csv")  
    data9 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-9.csv")
    data10 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-10.csv")
    data11 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-11.csv")
    data12 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-12.csv")
    data13 = qload(PATHS.rawfunds, FUND_FIELDS.cost, "part-13.csv")
end