using Revise
using DataFrames
using CSV
using Arrow
using Dates
using Statistics
using ShiftedArrays: lead, lag

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function process_mf_data()
    """
    The returned output is known to include all dates internal to the sample period.
    """
    task_start = time()
    data_filename = joinpath(DIRS.mf.init, "mf-data.arrow")
    info_filename = joinpath(DIRS.mf.raw, "info.csv")

    data = loadarrow(data_filename)
    info = init_raw(info_filename, info=true)

    active_data = _filter_out_passive(data, info)
    sort!(active_data, [:fundid, :date])
    aggregate_data = _aggregate_to_fundid(active_data)

    _null_out_small!(aggregate_data)
    _trim_missing_tails!(aggregate_data)
    _calculate_fund_flows!(aggregate_data)
    _clip_fund_flows!(aggregate_data)
    _filter_out_low_obs_funds!(aggregate_data)
    processed_data = _add_foreign_dummy_and_age(aggregate_data, info)

    rename!(processed_data, :net_returns => :ret)
    aggregate_data[:, [:ret, :costs]] ./= 100

    processed_data.std_return_12m = rolling_std(
        processed_data, :ret, 12;
        lagged=true, grouped_by=:fundid
    )

    output = select(
        processed_data, 
        [
            :fundid,
            :date,
            :flow,
            :ret,
            :costs,
            :net_assets_m1,
            :foreign,
            :age,
            :std_return_12m
        ]
    )
    printtime("processing mutual fund data", task_start, minutes=false)
    return output
end

function _filter_out_passive(data, info)
    passive_keywords = [
        "index",
        "idx",
        "etf",
        "s&p",
        "nasdaq",
        "dow",
        "russell"
    ]

    function matches_keywords(name, keywords)
        for kw in keywords
            if occursin(kw, lowercase(name)) && !startswith(lowercase(name), kw)
                return true
            end
        end
        return false
    end

    passive_fundids = info[
        matches_keywords.(info.fund_legal_name, Ref(passive_keywords)),
        :fundid
    ] |> Set

    active_data = data[.!in.(data.fundid, Ref(passive_fundids)), :]
    return active_data
end

function _aggregate_to_fundid(data)
    total_assets = combine(
        groupby(data, [:fundid, :date]),
        :net_assets => sum => :total_net_assets
    )

    data_total = innerjoin(data, total_assets, on=[:fundid, :date])
    data_total.lead_weight = data_total.net_assets ./ data_total.total_net_assets
    data_weighted = transform(
        groupby(data_total, :secid),
        :lead_weight => lag => :weight
    )
    
    data_weighted.weighted_net_returns = data_weighted.weight .* data_weighted.net_returns
    data_weighted.weighted_costs = data_weighted.weight .* data_weighted.costs

    aggregate_data = combine(
        groupby(data_weighted, [:fundid, :date]),
        :net_assets => sum => :net_assets,
        :weighted_net_returns => sum => :net_returns,
        :weighted_costs => sum => :costs
    )

    lagged_assets_aggregate_data = transform(
        groupby(aggregate_data, :fundid),
        :net_assets => lag => :net_assets_m1
    )

    return lagged_assets_aggregate_data
end

function _null_out_small!(data)
    data[
        coalesce.(data.net_assets_m1, 0) .< 10_000_000,
        [:net_assets_m1, :net_assets, :net_returns, :costs]
    ] .= missing

    return
end

function _trim_missing_tails!(data)
    tail_dates = DataFrame(
        fundid = String[],
        first_date = Date[],
        last_date = Date[]
    )

    gb = groupby(data, :fundid)
    
    for group in gb
        first_idx = findfirst(!ismissing, group.net_returns)
        isnothing(first_idx) && continue
        last_idx = findlast(!ismissing, group.net_returns)
        
        fundid = group.fundid[1]
        first_date = group[first_idx, :date]
        last_date = group[last_idx, :date]
        push!(tail_dates, (fundid, first_date, last_date))
    end
    leftjoin!(data, tail_dates, on=:fundid)
    
    filter!(
        row ->
            !ismissing(row.first_date) &&
            (row.date >= row.first_date) &&
            (row.date <= row.last_date),
        data
    )

    select!(data, Not(:first_date, :last_date))
    return
end

function _calculate_fund_flows!(data)
    data.flow = (data.net_assets ./ data.net_assets_m1) .- (1 .+ (data.net_returns ./ 100))
    return
end

function _clip_fund_flows!(data)
    flow_lowerbound = -0.9
    flow_upperbound = 10

    data[
        coalesce.(data.flow .<= flow_lowerbound,false) .||
        coalesce.(data.flow .>= flow_upperbound,false),
        [:net_assets, :net_returns, :costs, :net_assets_m1, :flow]
    ] .= missing
    return
end

function _filter_out_low_obs_funds!(data)
    fund_obs = combine(
        groupby(data, :fundid),
        :date => length => :nobs
    )

    valid_funds = fund_obs[fund_obs.nobs .>= 24, :fundid] |> Set
    filter!(row -> row.fundid in valid_funds, data)
    return
end

function _add_foreign_dummy_and_age(data, info)
    investment_target_cols = [
        :global_category, :morningstar_category, :us_category_group, :investment_area
    ]
    investment_target_info = info[:, [:fundid; investment_target_cols; :inception_date]]

    # First ensure that the retained fields don't differ for the same fundid before
    # selecting only the first row for each fundid.
    assert_similar_fundids(investment_target_info)
    fund_investment_targets = unique(investment_target_info, :fundid)

    target_data = innerjoin(data, fund_investment_targets, on=:fundid)
    target_data.foreign = investment_target_is(target_data, :wld)
    target_data.age = (
        12 .* (year.(target_data.date) .- year.(target_data.inception_date))
        .+ month.(target_data.date) .- month.(target_data.inception_date)
    )
    output = select(target_data, [propertynames(data); [:foreign, :age]])

    return output
end

if isnothing(match(r"terminalserver.jl$", abspath(PROGRAM_FILE)))
    output_data = process_mf_data()
    output_filename = makepath(DIRS.mf.refined, "mf-simple-returns.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing processed mutual fund data", task_start, minutes=false)
end