using Revise
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
    loadarrow(joinpath(DIRS.mf.refined, "mf-data.arrow"))
    info = loadarrow(joinpath(DIRS.mf.refined, "mf-info.arrow"))
    
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

    domestic_funds = info[domestic_condition(info),:]
    international_funds = info[international_condition(info),:]
    emerging_funds = info[emerging_condition(info),:]
    international_ex_emerging_funds = info[international_condition(info) .&& .!emerging_condition(info),:]
    

    domestic_funds

    [println(x) for x in (countmap(info.us_category_group) |> collect)]
    countmap(info[coalesce.(info.us_category_group .== "Sector Equity",false),:].investment_area)
    x = info[info.morningstar_category .== "US Fund Diversified Emerging Mkts",:]

    (info[coalesce.(info.investment_area .== "Global Emerging Mkts",false) .&& coalesce.(info.morningstar_category .!= "US Fund Diversified Emerging Mkts"),:])
    countmap(x.investment_area)
    info[coalesce.(info.us_category_group .== "International Equity",true),:]
    println(info[ismissing.(info.us_category_group),:])
    describe(info[coalesce.(in.(info.global_category, Ref(["Global Emerging Markets Equity", "Europe Emerging Markets Equity"])),false),:])

    println(sort(countmap(info.global_category) |> collect, by = x -> x[2], rev = true))

    old_betas_fn = joinpath(DIRS.test, "old-comparison-data/old-format/old_world_ff3_verdelhan_betas.arrow")

    betas = loadarrow(betas_fn)
    old_betas = loadarrow(old_betas_fn)

    describe(betas)
    describe(old_betas)

    idfilter = betas[!, [:fundid, :date, :factor]]
    old_betas.date = firstdayofmonth.(old_betas.date)

    filtered_betas = innerjoin(idfilter, old_betas, on = [:fundid, :date, :factor])
    rename!(betas, :coef => :new_coef)

    combo_data = innerjoin(filtered_betas, betas[!, [:fundid, :date, :new_coef, :factor]], on = [:fundid, :date, :factor])

    missing_data = combo_data[ismissing.(combo_data.new_coef), :]

    combo_data[combo_data.fundid .== "FS00008L0W", :]



    describe(betas)
end