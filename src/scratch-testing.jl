using DataFrames
using CSV
using Arrow
using GLM
using Dates
using StatsBase
using Base.Threads
using LinearAlgebra
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function test()
    betas_filename = joinpath(DIRS.combo.return_betas, "wld_ff3_ver.arrow")
    betas_filename_old = joinpath(DIRS.test, "old_world_ff3_verdelhan_betas.arrow")

    betas = loadarrow(betas_filename)
    old_betas = loadarrow(betas_filename_old)

    function compare_data(id_index, column=nothing; old_data=old_data, new_data=new_data)
        common_funds = intersect(old_data.fundid, new_data.fundid) |> collect

        if isnothing(column)
            show_cols = names(old_data)
        else
            column = Symbol.([column...])
            show_cols = vcat([:fundid, :date], column)
        end

        fundid = common_funds[id_index]
        old_data = old_data[old_data.fundid .== fundid, show_cols]
        new_data = new_data[new_data.fundid .== fundid, show_cols]

        old_data.date = firstdayofmonth.(old_data.date)

        println("Fund ID: ", fundid)
        println("")

        if !isnothing(column)
            combo_data = innerjoin(old_data, new_data, on=[:fundid, :date], makeunique=true)
            println(combo_data)
        else
            println("Old Data: \n\n", old_data)
            println("")
            println("New Data: \n\n", new_data)
        end
        
        println("")
    end

    compare_data(1, [:factor, :coef]; old_data=old_betas, new_data=betas)

    countmap(betas[betas.fundid .== "FS00008KSC",:coef])

   
   
    rates_filename = joinpath(DIRS.eq.raw, "country-mkt.csv")
    old_rates_filename = joinpath(DIRS.test, "old_usd_riskfree.csv")

    rates_data = CSV.read(rates_filename, DataFrame)
    old_rates = CSV.read(old_rates_filename, DataFrame, dateformat="dd/mm/yyyy")

    rates_data.new_rf = rates_data.mkt_vw - rates_data.mkt_vw_exc

    new_rates = rates_data[rates_data.excntry .== "USA", [:eom, :new_rf]]

    rename!(new_rates, :eom => :date)
    compared_rates = outerjoin(new_rates, old_rates, on=:date)

    dropmissing!(compared_rates)

    compared_rates.new_rf = compared_rates.new_rf .* 100

    compared_rates.comparison_rf = round.(compared_rates.new_rf, digits=2)


    compared_rates[compared_rates.rf .!= compared_rates.comparison_rf, :]

    cor(compared_rates.rf, compared_rates.comparison_rf)

    
    
    
    old_decomp_filename = joinpath(DIRS.test, "old_world_ff3_lrv.arrow")
    new_decomp_filename = joinpath(DIRS.combo.decomposed, "wld_ff3_lrv.arrow")
    old_excess_mf_filename = joinpath(DIRS.test, "old_excess_fund_data.arrow")
    new_excess_mf_filename = joinpath(DIRS.mf.refined, "mf-data.arrow")



    old_data = loadarrow(old_decomp_filename)
    new_data = loadarrow(new_decomp_filename)

    

    compare_data(2, :ret_alpha)



    old_excess_mf = loadarrow(old_excess_mf_filename)
    new_excess_mf = loadarrow(new_excess_mf_filename)

    old_data
    new_data

    test_id = new_excess_mf.fundid[findfirst(x->in(x,old_excess_mf.fundid), new_excess_mf.fundid)]

    old_excess_mf[old_excess_mf.fundid .== test_id, :]
    new_excess_mf[new_excess_mf.fundid .== test_id, :]
end