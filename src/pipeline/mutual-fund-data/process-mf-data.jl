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
    aggregate_info = _aggregate_info(info)

    _null_out_small!(aggregate_data)
    _trim_missing_tails!(aggregate_data)
    _calculate_fund_flows!(aggregate_data)
    _clip_fund_flows!(aggregate_data)
    _filter_out_low_obs_funds!(aggregate_data)
    processed_data = _add_info_data(aggregate_data, aggregate_info)

    sort!(processed_data, [:fundid, :date])

    rename!(
        processed_data,
        :net_returns => :ret,
        :true_no_load => :no_load
    )
    processed_data[:, [:ret, :costs]] ./= 100

    processed_data.std_return_12m = rolling_std(
        processed_data, :ret, 12;
        lagged=true, grouped_by=:fundid
    )

    output = (
        data=select(
            processed_data, 
            [
                :fundid,
                :date,
                :flow,
                :ret,
                :costs,
                :net_assets_m1,
                :no_load,
                :foreign,
                :age,
                :std_return_12m
            ]
        ),
        info=aggregate_info
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

function _aggregate_info(mf_info)
    match_if_equal(x) = length(unique(x)) == 1 ? first(x) : missing

    output = combine(
        groupby(mf_info, :fundid),
        :fund_standard_name => match_if_equal => :fund_standard_name,
        :fund_legal_name => match_if_equal => :fund_legal_name,
        :global_category => match_if_equal => :global_category,
        :morningstar_category => match_if_equal => :morningstar_category,
        :us_category_group => match_if_equal => :us_category_group,
        :investment_area => match_if_equal => :investment_area,
        :true_no_load => all => :true_no_load,
        :inception_date => minimum => :inception_date
    )

    return output
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

function _add_info_data(data, aggregate_info)
    """
    Adds the following columns to the data:
    - foreign: whether the fund invests primarily in foreign assets
    - age: the minimum age across share classes of the fund in months
    - true_no_load: whether all share classes of the fund are no-load
    """

    combined_data = innerjoin(data, aggregate_info, on=:fundid)
    combined_data.foreign = investment_target_is(combined_data, :wld)
    combined_data.age = (
        12 .* (year.(combined_data.date) .- year.(combined_data.inception_date))
        .+ month.(combined_data.date) .- month.(combined_data.inception_date)
    )

    output = select(combined_data, [propertynames(data); [:foreign, :age, :true_no_load]])

    return output
end

if isnothing(match(r"terminalserver.jl$", abspath(PROGRAM_FILE)))
    output_data = process_mf_data()
    output_filename_data = makepath(DIRS.mf.refined, "mf-simple-returns.arrow")
    output_filename_info = makepath(DIRS.mf.refined, "mf-info.arrow")

    task_start = time()
    Arrow.write(output_filename_data, output_data.data)
    Arrow.write(output_filename_info, output_data.info)
    printtime("writing processed mutual fund data", task_start, minutes=false)
end