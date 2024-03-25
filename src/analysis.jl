using Revise
using DataFrames
using CSV
using Arrow
using GLM
using Dates
using StatsBase
using Base.Threads
using LinearAlgebra
using Plots
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")
includet("pipeline/regressions/regress-fund-flows.jl")

using .CommonConstants
using .CommonFunctions
using .RegressFundFlows

function analysis()
    domestic_condition(x) = (
        (.!ismissing.(x.us_category_group) .&& (x.us_category_group .== "US Equity")) .||
        (.!ismissing.(x.investment_area) .&& (x.investment_area .== "United States of America"))
    )
    
    international_condition(x) = (
        (.!ismissing.(x.us_category_group) .&& (x.us_category_group .== "International Equity")) .||
        (.!ismissing.(x.investment_area) .&& (x.investment_area .!= "United States of America"))
    )

    emerging_condition(x) = (
        x.morningstar_category .== "US Fund Diversified Emerging Mkts" .||
        (.!ismissing.(x.investment_area) .&& (x.investment_area .== "Global Emerging Mkts"))
    )

    regress_fund_flows("usa_capm", filter_by=domestic_condition)
    regress_fund_flows("usa_ff3", filter_by=domestic_condition)
    regress_fund_flows("usa_ff3", filter_by=domestic_condition)
end

if abspath(PROGRAM_FILE) == @__FILE__
    analysis()
end