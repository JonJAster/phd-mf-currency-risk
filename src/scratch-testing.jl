using Revise
using BenchmarkTools
using DataFrames
using CSV
using REPL
using Arrow
using GLM
using Dates
using DataStructures
using StatsBase
using Base.Threads
using LinearAlgebra
using Plots
using Distributions
using ColorSchemes
using ShiftedArrays: lead, lag

includet("shared/CommonConstants.jl")
includet("shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions
Dates.lastdayofmonth
function test()
    # Crawl a group-class there
    data = loadarrow(joinpath(DIRS.mf.init, "mf-data.arrow"))
    info = loadarrow(joinpath(DIRS.mf.init, "mf-info.arrow"))
    eq_data = data[nonmissing(passmissing(startswith).(data.investment_objective, "E")), :]
    sort!(eq_data, :date)

    n_shared_classes = combine(
        groupby(eq_data, :class_group_id),
        :fund_class_id => (x->length(unique(skipmissing(x)))) => :n_shared_classes
    )
    dropmissing!(n_shared_classes)

    share_classes_counts = countmap(n_shared_classes.n_shared_classes) |> collect |> sort

    shared2 = n_shared_classes[n_shared_classes.n_shared_classes .== 2, :]
    shared5 = n_shared_classes[n_shared_classes.n_shared_classes .== 5, :]

    function crawl_fund_set(data, group_ids_0)
        group_iterations = OrderedDict{Int32, Union{Int64, Vector{Int64}}}(0 => group_ids_0)
        class_iterations = OrderedDict(
            0 => unique(fundgroup(data, group_ids_0).fund_class_id)
        )

        group_i = 0
        class_j = 0
        function grown(group_i, class_j)
            # class_j is 0 on entry into the loop and never again
            class_j == 0 && return true
    
            groups_grown = (
                length(group_iterations[group_i]) > length(group_iterations[group_i-1])
            )
            classes_grown = (
                length(class_iterations[class_j]) > length(class_iterations[class_j-1])
            )
            return groups_grown && classes_grown
        end
        while grown(group_i, class_j)
            subdata = fundclass(data, class_iterations[class_j])
            group_iterations[group_i+1] = unique(skipmissing(subdata.class_group_id))

            subdata = fundgroup(data, group_iterations[group_i+1])
            class_iterations[class_j+1] = unique(subdata.fund_class_id)

            group_i += 1
            class_j += 1
        end
        return group_iterations, class_iterations
    end

    testgroup = shared2.class_group_id[5]
    test_output = crawl_fund_set(eq_data, testgroup)
    max_itr_group = maximum(keys(test_output[1]))
    max_itr_class = maximum(keys(test_output[2]))
    testgroups = test_output[1][max_itr_group]
    testclasses = test_output[2][max_itr_class]
    testdata = eq_data[
            nonmissing(eq_data.class_group_id .∈ Ref(testgroups)) .||
            (eq_data.fund_class_id .∈ Ref(testclasses)),
            propertynames(eq_data)
    ]
    interested_cols = [
        :date,
        :class_group_id,
        :fund_class_id,
        :ret,
        :net_assets,
        :costs,
        :investment_objective,
        :fund_name
    ]
    inspected_data = leftjoin(
        testdata[:, interested_cols],
        info[!, [:fund_class_id, :recent_fund_name]],
        on=:fund_class_id
    )
    sort!(inspected_data, :date)
    pprint(inspected_data, color_by=:fund_class_id)

    data[(data.fund_class_id .== 30566) .&& (data.date .== Date(2001,11,30)),:]

    # Return data quality test
    data = loadarrow(joinpath(DIRS.mf.init, "mf-data.arrow"))
    eq_data = data[nonmissing(passmissing(startswith).(data.investment_objective, "E")), :]
    info = loadarrow(joinpath(DIRS.mf.init, "mf-info.arrow"))

    testidx = findfirst(x->!ismissing(x) && 0.00001<x<0.0001, eq_data.ret)

    eq_data[testidx-5:testidx+5, :]

    minimum(skipmissing(eq_data.ret))

    testid = eq_data[nonmissing(eq_data.ret .== -1),:fund_class_id]
    eq_data[eq_data.fund_class_id .== testid[1], :]


    length(unique(eq_data.fund_class_id))

    pprint(test_data)

    # Test IDs
    init_data = loadarrow(joinpath(DIRS.mf.init, "mf-data.arrow"))
    init_info = loadarrow(joinpath(DIRS.mf.init, "mf-info.arrow"))

    n_funds = combine(
        groupby(init_data, :class_group_id),
        :fund_class_id => (x->length(unique(skipmissing(x)))) => :n_funds
    )

    dropmissing!(n_funds)

    init_data = sort(init_data, :date)

    funds2 = n_funds[n_funds.n_funds .== 2, :class_group_id]
    funds2_stepper = stepgroup(init_data, funds2)

    pprint(datastep(funds2_stepper), centre=true, color_by=:fund_class_id)

    backids = [38239, 95245]
    pprint(fundclass(init_data, backids))

    value_i = iterate(funds2_stepper.itr)
    x = funds2_stepper.data[nonmissing(funds2_stepper.data.class_group_id .== funds2[1]),:]
    
    # Test iterators
    x = (1,3,5,7)
    a = iterate(x)

    it = Iterators.Stateful(["a", "b", "c"])
    propertynames(it)
    it.itr[it.nextvalstate[2]-2]

    isempty(it)
    iterate(it)
    iterate(it, ans[2])

    # Test IDs
    init_data = loadarrow(joinpath(DIRS.mf.init, "mf-data.arrow"))
    init_info = loadarrow(joinpath(DIRS.mf.init, "mf-info.arrow"))

    # How many class groups are associated with each class over its whole time series?
    n_groups = combine(
        groupby(init_data, :fund_class_id),
        :class_group_id => (x->length(unique(skipmissing(x)))) => :n_groups
    )

    describe(n_groups)

    max_n = 12
    test_id = n_groups.fund_class_id[n_groups.n_groups .== max_n][1]

    test_data = init_data[init_data.fund_class_id .== test_id, :]
    test_info = init_info[init_info.fund_class_id .== test_id, :]
    test_name = test_info.fund_name[1]

    group_list = unique(skipmissing(test_data.class_group_id))

    for group in group_list
        # group = group_list[1]
        group_data = test_data[coalesce.(test_data.class_group_id .== group, false), :]
        println(group, " ", length(unique(group_data.fund_class_id)))
    end

    pprint(test_data)

    # How many class groups are only ever associated with exactly one fund?
    n_funds = combine(
        groupby(init_data, :class_group_id),
        :fund_class_id => (x->length(unique(skipmissing(x)))) => :n_funds
    )

    dropmissing!(n_funds)
    describe(n_funds)
    singletons = n_funds[n_funds.n_funds .== 1, :]

    n_singletons = nrow(singletons)
    total_funds = length(unique(init_data.fund_class_id))
    println(n_singletons, " out of ", total_funds, " ($(round(n_singletons/total_funds*100,digits=2))%) class groups are singletons")

    funds2 = n_funds[n_funds.n_funds .== 2, :]

    pprint(data[nonmissing(data.class_group_id .== funds2.class_group_id[1]),:])

    ## Start CRSP data era tests
    fund_header = loadarrow(joinpath(DIRS.mf.raw, "fund_hdr.arrow"))

    x = fund_header[coalesce.((fund_header.retail_fund .== "N"),false) .&& coalesce.((fund_header.inst_fund .== "N"),false), :]

    pprint(x[1:5,:])

    println(x[end-20:end, 1:7])

    fund_header_hist = loadarrow(joinpath(DIRS.mf.raw, "fund_hdr_hist.arrow"))

    class_counts = combine(
        groupby(fund_header, :crsp_cl_grp),
        :retail_fund => (x->count(==("Y"), skipmissing(x))) => :retail_fund_count,
        :inst_fund => (x->count(==("Y"), skipmissing(x))) => :inst_fund_count
    )

    dropmissing!(class_counts)
    class_counts.both_flag = (class_counts.retail_fund_count .* class_counts.inst_fund_count) .> 0

    class_counts[class_counts.both_flag, :]

    pprint(fund_header[coalesce.(fund_header.crsp_cl_grp .== 2000010,false), [:fund_name, :retail_fund, :inst_fund]])

    nunique_crsp_cl_grp = combine(
        groupby(fund_header_hist, :crsp_fundno),
        :crsp_cl_grp => (x->length(unique(skipmissing(x)))) => :nunique_crsp_cl_grp
    )

    test_fundno = nunique_crsp_cl_grp[nunique_crsp_cl_grp.nunique_crsp_cl_grp .== maximum(nunique_crsp_cl_grp.nunique_crsp_cl_grp), :crsp_fundno][1]

    fund_header[fund_header.crsp_fundno .== test_fundno, :]
    fund_header_hist[fund_header_hist.crsp_fundno .== test_fundno, :]

    test_hist = fund_header_hist[fund_header_hist.crsp_fundno .== test_fundno, 1:7]

    println(test_hist)

    grp_ids = Int.(unique(skipmissing(test_hist.crsp_cl_grp)))

    for id in grp_ids
        println(fund_header_hist[coalesce.(fund_header_hist.crsp_cl_grp .== id,false), 1:7])
    end



    nmissing = OrderedDict(
        col => count(ismissing, fund_header[!, col]) for col in propertynames(fund_header)
    )

    list_nmissing = sort(collect(nmissing), by=x->x[2])
    for (field, nmissing) in list_nmissing
        println("$field: $nmissing")
    end

    function inspect_missing(field)
        message = (
            "$(round(nmissing[:crsp_portno]/1000; digits=1))k missing from " *
            "$(round(nrow(fund_header)/1000; digits=1))k total records " *
            "[$(round(nmissing[:crsp_portno]/nrow(fund_header)*100, digits=2))%]"
        )

        return message
    end

    inspect_missing(:fund_name)

        


    ## Start Morningstar data era tests
    # Testing fees
    init_data = loadarrow(joinpath(DIRS.mf.init, "mf-data.arrow"))
    
    fee_data = dropmissing(init_data, [:costs, :net_assets])
    total_assets = combine(
        groupby(fee_data, :date),
        :net_assets => sum => :total_assets
    )

    merged_data = innerjoin(fee_data, total_assets, on=:date)

    fee_data.weighted_fees = fee_data.costs .* fee_data.net_assets ./ merged_data.total_assets

    weighted_fees = combine(
        groupby(fee_data, :date),
        :weighted_fees => sum => :weighted_fees
    )

    mean(weighted_fees.weighted_fees)

    
    # Testing
    qlookup("FSUSA0099L")
    # Testing for similar gross returns across fund odd_class_name_ids
    init_data = loadarrow(joinpath(DIRS.mf.init, "mf-data.arrow"))

    cross_class_var = combine(
        groupby(init_data, [:fundid, :date]),
        :gross_returns => (x->(length(unique(skipmissing(x)))>1 ? var(skipmissing(x)) : 0)) => :cross_class_var
    )

    describe(cross_class_var)

    sort!(cross_class_var, :cross_class_var, rev=true)

    test_case = 6
    test_fundid = cross_class_var.fundid[test_case]
    test_date = cross_class_var.date[test_case]
    test_data = init_data[(init_data.fundid .== test_fundid) .& (init_data.date .== test_date), :]
    test_data_full = init_data[init_data.fundid .== test_fundid, :]

    dropmissing!(test_data_full, :gross_returns)
    describe(test_data_full)

    sort!(test_data_full, [:secid, :date])

    class_plot = plot(
        xlabel="Date",
        ylabel="Gross Returns",
        title="Gross Returns of Funds in the Same Class"
    )
    for fund in unique(test_data_full.secid)
        fund_data = test_data_full[test_data_full.secid .== fund, :]
        plot!(
            fund_data.date, fund_data.gross_returns, label=fund;
            linewidth=5,
            alpha=0.2
        )
        display(class_plot)
    end

    pprint(qlookup(test_fundid))

    # pair-wise correlation table of all secid gross returns for test fund
    wide_grets = unstack(test_data_full, :date, :secid, :gross_returns)
    test_secids = unique(test_data_full.secid)
    for i in test_secids
        for j in test_secids
            i == j && continue
            test_cor = wide_grets[completecases(wide_grets[!, [i,j]]), [i,j]]
            isempty(test_cor) && continue
            println(i, " ", j, " ", cor(test_cor[!,i], test_cor[!,j]))
        end
    end
    

    # Selecting a fund today
    fund_data = loadarrow(joinpath(DIRS.mf.refined, "mf-excess-returns.arrow"))
    current_funds = fund_data[fund_data.date .== maximum(fund_data.date), :]
    info = loadarrow(joinpath(DIRS.mf.init, "mf-info.arrow"))
    duplicate_values = combine(
        groupby(info, :fundid),
        propertynames(info)[2:end] .=> (x->length(unique(x)))
    )
    rename!(duplicate_values, propertynames(info))
    for i in propertynames(duplicate_values[!, Not(:fundid, :secid)])
        if sum(duplicate_values[!, i] .== duplicate_values.secid) == nrow(duplicate_values)
            println("$i matches secid")
        elseif sum(duplicate_values[!, i] .== 1) == nrow(duplicate_values)
            println("$i is unique to fundid")
        else
            println("$i is odd")
        end
    end

    odd_class_name_ids = (
        duplicate_values[duplicate_values.fund_class_name .!= duplicate_values.secid, :fundid]
    )

    pprint(info[in.(info.fundid, Ref(odd_class_name_ids)), Not(:fund_class_name, :fund_standard_name, :management_approach_passive, :management_approach_active)])

    odd_class_legal_name_ids = (
        duplicate_values[duplicate_values.fund_class_legal_name .!= duplicate_values.secid, :fundid]
    )

    pprint(sort(info[in.(info.fundid, Ref(odd_class_legal_name_ids)), Not(:fund_standard_name, :management_approach_passive, :management_approach_active)], :fundid), rows=50)

    duplicate_values[duplicate_values.fundid .== "FSUSA004K0", :]

    displaysize(stdout)[2]

    func

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
        # println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m2"]))
        # println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m3"]))
        # println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m4"]))
        # println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m5"]))
        # println(cor(decomposed_rets[!, i], decomposed_rets[!, "$(i)_m6"]))
    end

    for d in unique(decomposed_rets.date)
        decomposed_rets[!, Symbol(d)] .= decomposed_rets.date .== d
    end

    for i in ret_cols
        reg_formula = term(i) ~ sum(term.(vcat([Symbol("$(i)_m1")], Symbol.(sort(unique(decomposed_rets.date)))[2:end])))
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