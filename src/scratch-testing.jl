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

using .CommonConstants
using .CommonFunctions

function test()
    test_data_fn = joinpath(DIRS.test, "old-comparison-data/new-format/magnitude-adjusted/dev_ff3_ver.arrow")
    test_data = loadarrow(test_data_fn)
    test_data = test_data[test_data.fundid .== "FS00008L0W", :]
    CSV.write(joinpath(DIRS.test, "handtest_decomposition_betas.csv"), test_data)


    old_data_old_method_fn = joinpath(DIRS.test, "old-comparison-data/old-format/old_world_ff3_verdelhan_decomposition.arrow")
    old_data_new_method_fn = joinpath(DIRS.test, "old-comparison-data/new-format/magnitude-adjusted/decomposed/dev_ff3_ver.arrow")

    old_data_old_method_fn = joinpath(DIRS.test, "old-comparison-data/old-format/old_world_ff3_verdelhan_weighted_decompositions.arrow")
    old_data_new_method_fn = joinpath(DIRS.test, "old-comparison-data/new-format/magnitude-adjusted/weighted/dev_ff3_ver.arrow")

    old_data_old_method = loadarrow(old_data_old_method_fn)
    old_data_new_method = loadarrow(old_data_new_method_fn)

    testid = "FS00008L0W"
    testdate = Date(2018,2,1)

    old_data_old_method[(old_data_old_method.fundid .== testid) .&& (firstdayofmonth.(old_data_old_method.date) .== testdate), :]
    old_data_new_method[(old_data_new_method.fundid .== testid) .&& (firstdayofmonth.(old_data_new_method.date) .== testdate), :]
        
    old_data_new_method[old_data_new_method.fundid .== testid, :]

    weighted_rets[2:2, :]
    old_weighted_rets[1:1, :]
    
    
    combo_factors_fn = joinpath(DIRS.combo.factors, "factors.arrow")

    combo_factors = loadarrow(combo_factors_fn)
    println(unique(combo_factors.factor))
    
    old_equity_factors = loadarrow(joinpath(DIRS.test, "old-comparison-data/old-format/old_global_equity_factors.arrow"))

    combo_factors_compare = combo_factors[in.(combo_factors.factor, Ref(["smb", "hml", "cma", "rmw", "wml", "mkt"])), :]
    combo_factors_compare = combo_factors_compare[combo_factors_compare.region .== "DEV", :]

    select!(combo_factors_compare, Not(:region))

    wide_new_factors = unstack(combo_factors_compare, :factor, :ret)
    select!(wide_new_factors, [:date, :smb, :hml, :cma, :rmw, :wml, :mkt])

    wide_new_factors = wide_new_factors[wide_new_factors.date .>= Date(1990, 7, 1), :]
    wide_new_factors
    old_equity_factors

    function compare_line(line; old=old_equity_factors, new=wide_new_factors)
        comparison_df = vcat(old[line:line, :], new[line:line, :])
        return comparison_df
    end

    compare_line(1)
    dev_mkt = combo_factors[(combo_factors.factor .== "mkt") .&& (combo_factors.region .=="DEV"), :]

    compare_mkt_fn = joinpath(DIRS.test, "old-comparison-data/old-format/old_dev_mkt.csv")
    compare_mkt = CSV.read(compare_mkt_fn, DataFrame, dateformat="dd/mm/yyyy")

    dated_dev_mkt = dev_mkt[dev_mkt.date .>= Date(1990, 7, 1),:]
    first(dated_dev_mkt, 5)

    compare_mkt.date = firstdayofmonth.(compare_mkt.date)
    first(compare_mkt, 5)

    combo_mkt = outerjoin(first(dated_dev_mkt, 5), first(compare_mkt, 5), on=:date)
    rf_fn = joinpath(DIRS.test, "old-comparison-data/old-format/old_usd_riskfree.csv")
    old_rf = CSV.read(rf_fn, DataFrame, dateformat="dd/mm/yyyy")
    old_rf.date = firstdayofmonth.(old_rf.date)

    combo_mkt_rf = innerjoin(combo_mkt, old_rf, on=:date)

    combo_mkt_rf[!, [:mkt, :rf]] ./= 100

    combo_mkt_rf.compare_ret = combo_mkt_rf.mkt .- combo_mkt_rf.rf

    combo_mkt_rf

    old_format_folder = joinpath(DIRS.test, "old-comparison-data/old-format")
    old_data_filename = joinpath(old_format_folder, "old_world_ff3_verdelhan_betas.arrow")
    new_data_filename = joinpath(DIRS.combo.return_betas, "dev_ff3_ver.arrow")

    old_data = loadarrow(old_data_filename)
    new_data = loadarrow(new_data_filename)

    new_data = dropmissing(new_data)

    old_data[old_data.factor .== :const, :]
    dropmissing(new_data[new_data.factor .== :const, :])

    testid = old_data.fundid[findfirst(x->in(x,new_data.fundid), old_data.fundid)]

    old_data[old_data.fundid .== testid, :]
    new_data[new_data.fundid .== testid, :]






    findfirst(x->in(x,old_data.fundid), new_data.fundid)
    testid = new_data.fundid[findfirst(x->in(x,old_data.fundid), new_data.fundid)]

    old_data[old_data.fundid .== testid, :]
    
    loadarrow(joinpath(DIRS.test, "old-comparison-data/new-format/mf-data.arrow"))
    old_data.fund_assets_m1 = lag(old_data.fund_assets)

    old_dev_mkt_filename = joinpath(DIRS.test, "old_dev_mkt.csv")
    new_mkt_filename = joinpath(DIRS.eq.factors, "mkt.arrow")

    old_dev_mkt = CSV.read(old_dev_mkt_filename, DataFrame, dateformat="dd/mm/yyyy")
    new_mkt = loadarrow(new_mkt_filename)
    
    old_dev_mkt.date .= firstdayofmonth.(old_dev_mkt.date)
    new_mkt_wide = unstack(new_mkt, :region, :mkt_exc)
    new_dev_wld_mkt = new_mkt_wide[!, [:date, :WLD, :DEV]]
    compare_mkt = innerjoin(old_dev_mkt, new_dev_wld_mkt, on=:date)

    rename!(compare_mkt, :mkt=>:OLD_DEV)

    compare_mkt.OLD_DEV ./= 100

    plot(compare_mkt.date, [compare_mkt.WLD, compare_mkt.OLD_DEV])

    compare_mkt.deviation_for_wld = (compare_mkt.WLD .- compare_mkt.OLD_DEV)

    plot(compare_mkt.date, compare_mkt.deviation_for_wld, label="Deviation for WLD")

    cor(compare_mkt.DEV, compare_mkt.OLD_DEV)

    plot(compare_mkt.date, [compare_mkt.DEV, compare_mkt.OLD_DEV], label=["New Dev" "Old Dev"])

    compare_mkt.deviation_for_dev = (compare_mkt.DEV .- compare_mkt.OLD_DEV)

    plot(compare_mkt.date, compare_mkt.deviation_for_wld, label="Deviation for WLD")
    plot!(compare_mkt.date, compare_mkt.deviation_for_dev, label="Deviation for Dev")



    
    
    
    
    
    
    
    
    
    
    
    
    
    
    betas_filename = joinpath(DIRS.combo.return_betas, "dev_ff3_ver.arrow")
    betas_filename_old = joinpath(DIRS.test, "old_world_ff3_verdelhan_betas.arrow")

    betas = loadarrow(betas_filename)
    old_betas = loadarrow(betas_filename_old)

    # regression_data = initialise_base_data(MODELS["wld_ff3_ver"])

    # CSV.write(joinpath(DIRS.test, "beta_handtest.csv"), regression_data)

    function compare_data(id_index, column=nothing; join_cols=[:fundid, :date], old_data=old_data, new_data=new_data, returns=false)
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
            combo_data = outerjoin(old_data, new_data, on=join_cols, makeunique=true)
            sort!(combo_data, [:fundid, :date])
            if returns
                return combo_data
            else
                println(combo_data)
            end
        else
            println("Old Data: \n\n", old_data)
            println("")
            println("New Data: \n\n", new_data)
        end
        
        println("")
    end

    test_data = compare_data(5, [:factor, :coef]; join_cols=[:fundid, :date, :factor], old_data=old_betas, new_data=betas, returns=true)
    dropmissing!(test_data)
    test_data_old = test_data[!, [:date, :factor, :coef]]
    test_data_new = test_data[!, [:date, :factor, :coef_1]]

    test_old_wide = unstack(test_data_old, :factor, :coef)
    test_new_wide = unstack(test_data_new, :factor, :coef_1)

    for factor in names(test_old_wide[:, Not(:date)])
        println(factor, ": ", cor(test_old_wide[!, factor], test_new_wide[!, factor]))
        display(
            plot(
                test_old_wide.date,
                [test_old_wide[!, factor], test_new_wide[!, factor]],
                label=["Old $factor" "New $factor"],
                title=factor
            )
        )
    end

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