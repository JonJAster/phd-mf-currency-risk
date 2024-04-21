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
    # Persistence of decomposed return components
    decomposed_rets = loadarrow(joinpath(DIRS.combo.decomposed, "ff_dev_ffc6.arrow"))
    ret_cols = names(decomposed_rets[!, Not([:fundid, :date])])
    for i in ret_cols
        decomposed_rets = transform(
            groupby(decomposed_rets, :fundid),
            i => (x->lag(x, 1)) => "$(i)_m1"#,
            # i => (x->lag(x, 2)) => "$(i)_m2",
            # i => (x->lag(x, 3)) => "$(i)_m3",
            # i => (x->lag(x, 4)) => "$(i)_m4",
            # i => (x->lag(x, 5)) => "$(i)_m5",
            # i => (x->lag(x, 6)) => "$(i)_m6",
        )
    end

    dropmissing!(decomposed_rets)

    for i in ret_cols
        println(i)
        println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m1"]))
        println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m2"]))
        println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m3"]))
        println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m4"]))
        println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m5"]))
        println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m6"]))
    end

    

    for i in ret_cols
        reg_formula = term(i) ~ sum([term("$(i)_m$j") for j in [1]])
        model = lm(reg_formula, decomposed_rets)
        display(model)
    end

    # Description of fund size
    fund_data = loadarrow(joinpath(DIRS.mf.refined, "mf-excess-returns.arrow"))
    describe(fund_data)

    monthly_flow_data = combine(
        groupby(fund_data, :date),
        :flow => (x->mean(skipmissing(x))) => :flow
    )

    sort!(monthly_flow_data, :date)
    monthly_flow_data.flow .*= 100

    plot(
        monthly_flow_data.date,
        monthly_flow_data.flow;
        xlabel="Date",
        ylabel="Flow (%)",
        legend=false,
        title="Monthly Flows of Mutual Funds",
        color=:darkblue,
        linewidth=1,
    )
    plot!(monthly_flow_data.date, fill(0, length(monthly_flow_data.date)), color=:red, alpha=0.2, linewidth=2)



    yearly_flow_data = DataFrame(year=Int64[], annual_flow=Float64[])
    for y in sort(unique(year.(fund_data.date)))
        flow_y = fund_data[(fund_data.date .>= Date(y,1,1)) .& (fund_data.date .<= Date(y,12,1)), [:fundid, :flow]]
        dropmissing!(flow_y)
        annual_flows = combine(
            groupby(flow_y, :fundid),
            :flow => (x->mean((x))) => :annual_flow
        )
        maximum(annual_flows.annual_flow)
        flow_output = round(((1+mean(annual_flows.annual_flow))^12-1)*100, digits=2)
        push!(yearly_flow_data, (y, flow_output))
    end

    plot(
        yearly_flow_data.year,
        yearly_flow_data.annual_flow;
        xlabel="Year",
        ylabel="Annual Flow (%)",
        legend=false,
        title="Annual Flows of Mutual Funds",
        color=:darkblue,
        linewidth=2,
    )
    plot!(yearly_flow_data.year, fill(0, length(yearly_flow_data.year)), color=:red, alpha=0.2, linewidth=2)

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

    println(agg_returns(data, :morningstar_category))

    data[startswith.(data.morningstar_category, Ref("EAA Fund")), :]

    testcase = (
        fundid = "FSUSA0BCMX",
        date = Date(2015,12,1),
        alpha = data[(data.fundid .== "FSUSA0BCMX") .&& data.date .== Date(2015,12,1), :usa_ff3_alpha][1]
    )

    qlookup(testcase.fundid)

    alphas[(alphas.fundid .== testcase.fundid) .&& (alphas.date .== testcase.date), :]

    fund_data[(fund_data.fundid .== testcase.fundid) .&& (fund_data.date .== (testcase.date-Month(1))), :]

    # Do fund returns display an efficient frontier?
    fund_data = loadarrow(joinpath(DIRS.mf.refined, "mf-excess-returns.arrow"))
    fund_data.year = year.(fund_data.date)

    function annualise_returns(year_of_returns)
        nonmissing_obs = count(!ismissing, year_of_returns)
        if nonmissing_obs == 12
            return (100).*(prod((1).+year_of_returns).-1)
        else
            return missing
        end
    end

    mf_annual = combine(
        groupby(fund_data, [:fundid, :year]),
        :ex_ret => annualise_returns => :annual_ex_ret
    )

    dropmissing!(mf_annual)

    mf_agg = combine(
        groupby(mf_annual, :fundid),
        :annual_ex_ret => mean => :mean_annual_ex_ret
    )

    mf_std = combine(
        groupby(fund_data, :fundid),
        :ex_ret => (x->std(x).*sqrt(12)) => :std_annual_ex_ret
    )

    mf_agg = innerjoin(mf_agg, mf_std, on=:fundid)
    dropmissing!(mf_agg)

    scatter(
        mf_agg.std_annual_ex_ret,
        mf_agg.mean_annual_ex_ret;
        xlabel="Standard Deviation",
        ylabel="Mean",
        legend=false,
        title="Efficient Frontier of Mutual Funds",
        alpha=0.5,
        markersize=2,
        markershape=:cross,
        color=:darkblue,
        ylims=(-25,30),
        xlims=(0,0.4),
        framestyle=:origin
    )




end