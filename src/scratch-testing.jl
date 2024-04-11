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
using Distributions
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function test()
    # Do funds of various subcategories earn average positive gross returns?
    fund_data = loadarrow(joinpath(DIRS.mf.refined, "mf-excess-returns.arrow"))
    info = loadarrow(joinpath(DIRS.mf.init, "mf-info.arrow"))

    betas = loadarrow(joinpath(DIRS.combo.return_betas, "ff_usa_ff3.arrow"))
    alphas = betas[betas.factor .== :const, [:fundid, :date, :coef]]
    dropmissing!(alphas)
    rename!(alphas, :coef => :usa_ff3_alpha)

    select!(info, [:fundid, :morningstar_category, :global_category])
    info = unique(deepcopy(info), :fundid)

    data = innerjoin(fund_data, info, on=:fundid)
    data = innerjoin(data, alphas, on=[:fundid, :date])

    function agg_returns(data, category=nothing)
        if isnothing(category)
            category = :category
            data = copy(data)
            data.category .= "All Funds"
        end
        
        df_agg = combine(
            groupby(data, category),
            :ex_ret => (x->mean(skipmissing(x))) => :mean_gross_excess_return,
            :ex_ret => (x->std(skipmissing(x))/sqrt(count(!ismissing,x))) => :se_gross_excess_return,
            :usa_ff3_alpha => (x->mean(skipmissing(x))) => :mean_usa_ff3_alpha,
            :usa_ff3_alpha => (x->std(skipmissing(x))/sqrt(count(!ismissing,x))) => :se_usa_ff3_alpha
        )

        df_agg.t_gross_excess_return = df_agg.mean_gross_excess_return ./ df_agg.se_gross_excess_return
        df_agg.t_usa_ff3_alpha = df_agg.mean_usa_ff3_alpha ./ df_agg.se_usa_ff3_alpha

        df_agg.p_gross_excess_return = 2 * (1 .- cdf(TDist(length(data.ex_ret) - 1), abs.(df_agg.t_gross_excess_return)))
        df_agg.p_usa_ff3_alpha = 2 * (1 .- cdf(TDist(length(data.usa_ff3_alpha) - 1), abs.(df_agg.t_usa_ff3_alpha)))

        df_agg[!, [:mean_gross_excess_return, :mean_usa_ff3_alpha]] = (
            round.((((1).+df_agg[!, [:mean_gross_excess_return, :mean_usa_ff3_alpha]]) .^ 12 .- 1).*100, digits=2)
        )
        select!(df_agg, [category, :mean_gross_excess_return, :p_gross_excess_return, :mean_usa_ff3_alpha, :p_usa_ff3_alpha])

        sort!(df_agg, :mean_usa_ff3_alpha, rev=true)

        return df_agg
    end

    morningstar_category_returns = agg_returns(data, :morningstar_category)
    global_category_returns = agg_returns(data, :global_category)
    total_agg = agg_returns(data)

    println(global_category_returns)
    println(morningstar_category_returns)
    println(total_agg)

    println(agg_returns(data[data.date .<= Date(1995, 12, 31), :], :morningstar_category))

    data[startswith.(data.morningstar_category, Ref("EAA Fund")), :]

    testcase = (
        fundid = "FSUSA0BCMX",
        date = Date(2015,12,1),
        alpha = data[(data.fundid .== "FSUSA0BCMX") .&& data.date .== Date(2015,12,1), :usa_ff3_alpha][1]
    )

    qlookup(testcase.fundid)

    alphas[(alphas.fundid .== testcase.fundid) .&& (alphas.date .== testcase.date), :]

    fund_data[(fund_data.fundid .== testcase.fundid) .&& (fund_data.date .== (testcase.date-Month(1))), :]
end