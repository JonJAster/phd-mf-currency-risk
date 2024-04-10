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
    factors_data = loadarrow(joinpath(DIRS.combo.factors, "factors.arrow"))
    mf_data = loadarrow(joinpath(DIRS.mf.refined, "mf-excess-returns.arrow"))

    function compare_betas(model_name_suffix, factor)
        factor = Symbol(factor)
        ff_model_name = "ff_$model_name_suffix"
        jkp_model_name = "jkp_$model_name_suffix"
        
        ff_betas = loadarrow(joinpath(DIRS.combo.return_betas, "$ff_model_name.arrow"))
        ff_factor = ff_betas[ff_betas.factor .== factor, :]
        select!(ff_factor, [:fundid, :date, :coef])
        dropmissing!(ff_factor)

        jkp_betas = loadarrow(joinpath(DIRS.combo.return_betas, "$jkp_model_name.arrow"))
        jkp_factor = jkp_betas[jkp_betas.factor .== factor, :]
        select!(jkp_factor, [:fundid, :date, :coef])
        dropmissing!(jkp_factor)

        compare_df = innerjoin(
            ff_factor,
            jkp_factor;
            on = [:fundid, :date],
            renamecols = "_ff" => "_jkp"
        )

        compare_df = sort(compare_df[compare_df.date .>= Date(1990,1,1), :], [:fundid, :date])

        println("Correlation between ff and jkp $factor betas: ", cor(compare_df.coef_ff, compare_df.coef_jkp))
        println()
        println("Summary statistics for $factor betas:")
        println("FF")
        describe(compare_df.coef_ff)
        println()
        println("JKP")
        describe(compare_df.coef_jkp)
        println()

        plot(compare_df.coef_ff, compare_df.coef_jkp, seriestype = :scatter, title = factor, xlabel = "ff", ylabel = "jkp") |> display
        # plot(
        #     plot(compare_df.coef_ff, compare_df.coef_jkp, seriestype = :scatter, title = factor, xlabel = "ff", ylabel = "jkp"),
        #     plot(compare_df.date, [compare_df.coef_ff compare_df.coef_jkp], label = ["ff" "jkp"], title = factor, xlabel = "Date", ylabel = "Betas"),
        #     layout = (2, 1),
        #     size = (800, 800)
        # )

        return compare_df
    end

    compare_betas("dev_ffc6", "hml")

    ###

    ff_usa = factors_data[factors_data.source_id .== "ff_usa", :]
    jkp_usa = factors_data[factors_data.source_id .== "jkp_usa", :]

    ff_dev = factors_data[factors_data.source_id .== "ff_dev", :]
    jkp_dev = factors_data[factors_data.source_id .== "jkp_dev", :]

    compare_mkt_usa = innerjoin(
        ff_usa[ff_usa.factor .== "mkt", Not(:source_id, :factor)],
        jkp_usa[jkp_usa.factor .== "mkt", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_smb_usa = innerjoin(
        ff_usa[ff_usa.factor .== "smb", Not(:source_id, :factor)],
        jkp_usa[jkp_usa.factor .== "smb", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_hml_usa = innerjoin(
        ff_usa[ff_usa.factor .== "hml", Not(:source_id, :factor)],
        jkp_usa[jkp_usa.factor .== "hml", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_wml_usa = innerjoin(
        ff_usa[ff_usa.factor .== "wml", Not(:source_id, :factor)],
        jkp_usa[jkp_usa.factor .== "wml", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_cma_usa = innerjoin(
        ff_usa[ff_usa.factor .== "cma", Not(:source_id, :factor)],
        jkp_usa[jkp_usa.factor .== "cma", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_rmw_usa = innerjoin(
        ff_usa[ff_usa.factor .== "rmw", Not(:source_id, :factor)],
        jkp_usa[jkp_usa.factor .== "rmw", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_mkt_dev = innerjoin(
        ff_dev[ff_dev.factor .== "mkt", Not(:source_id, :factor)],
        jkp_dev[jkp_dev.factor .== "mkt", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_smb_dev = innerjoin(
        ff_dev[ff_dev.factor .== "smb", Not(:source_id, :factor)],
        jkp_dev[jkp_dev.factor .== "smb", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_hml_dev = innerjoin(
        ff_dev[ff_dev.factor .== "hml", Not(:source_id, :factor)],
        jkp_dev[jkp_dev.factor .== "hml", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_wml_dev = innerjoin(
        ff_dev[ff_dev.factor .== "wml", Not(:source_id, :factor)],
        jkp_dev[jkp_dev.factor .== "wml", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_cma_dev = innerjoin(
        ff_dev[ff_dev.factor .== "cma", Not(:source_id, :factor)],
        jkp_dev[jkp_dev.factor .== "cma", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    compare_rmw_dev = innerjoin(
        ff_dev[ff_dev.factor .== "rmw", Not(:source_id, :factor)],
        jkp_dev[jkp_dev.factor .== "rmw", Not(:source_id, :factor)];
        on = :date,
        renamecols = "_ff" => "_jkp"
    )

    function show_comparison(compare_df, factor_name)
        compare_df = sort(compare_df[compare_df.date .>= Date(1990,1,1), :], :date)
        println("Correlation between ff and jkp $factor_name: ", cor(compare_df.ret_ff, compare_df.ret_jkp))
        
        plot(
            plot(compare_df.ret_ff, compare_df.ret_jkp, seriestype = :scatter, title = factor_name, xlabel = "ff", ylabel = "jkp"),
            plot(compare_df.date, [compare_df.ret_ff compare_df.ret_jkp], label = ["ff" "jkp"], title = factor_name, xlabel = "Date", ylabel = "Returns"),
            layout = (2, 1),
            size = (800, 800)
        )
    end

    show_comparison(compare_mkt_usa, "mkt")
    show_comparison(compare_smb_usa, "smb")
    show_comparison(compare_hml_usa, "hml")
    show_comparison(compare_wml_usa, "wml")
    show_comparison(compare_cma_usa, "cma")
    show_comparison(compare_rmw_usa, "rmw")

    show_comparison(compare_mkt_dev, "mkt")
    show_comparison(compare_smb_dev, "smb")
    show_comparison(compare_hml_dev, "hml")
    show_comparison(compare_wml_dev, "wml")
    show_comparison(compare_cma_dev, "cma")
    show_comparison(compare_rmw_dev, "rmw")
end