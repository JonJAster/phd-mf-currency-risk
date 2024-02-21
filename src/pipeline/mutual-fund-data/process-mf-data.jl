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
    task_start = time()
    data_filename = joinpath(DIRS.mf.init, "mf-data.arrow")
    info_filename = joinpath(DIRS.mf.init, "mf-info.arrow")
    market_filename = joinpath(DIRS.eq.raw, "country-mkt.csv")

    data = Arrow.Table(data_filename) |> DataFrame
    info = Arrow.Table(info_filename) |> DataFrame
    market_returns = CSV.read(market_filename, DataFrame, dateformat="yyyy-mm-dd")

    active_data = _filter_out_passive(data, info)
    sort!(active_data, [:fundid, :date])
    aggregate_data = _aggregate_to_fundid(active_data)

    _null_out_small!(aggregate_data)
    _trim_missing_tails!(aggregate_data)
    _calculate_fund_flows!(aggregate_data)
    _windsorise_fund_flows!(aggregate_data)
    _filter_out_low_obs_funds!(aggregate_data)
    
    riskfree = _calculate_riskfree(market_returns)

    full_data = innerjoin(aggregate_data, riskfree, on=:date)
    full_data.ex_ret = full_data.gross_returns - full_data.rf

    output = select(
        full_data, 
        [:fundid, :date, :flow, :ex_ret, :costs, :net_assets_m1]
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
    data_weighted.weighted_gross_returns = data_weighted.weight .* data_weighted.gross_returns
    data_weighted.weighted_costs = data_weighted.weight .* data_weighted.costs

    aggregate_data = combine(
        groupby(data_weighted, [:fundid, :date]),
        :net_assets => sum => :net_assets,
        :weighted_net_returns => sum => :net_returns,
        :weighted_gross_returns => sum => :gross_returns,
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
        [:net_assets_m1, :net_assets, :net_returns, :gross_returns, :costs]
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
    data.flow = (
        100 .* (data.net_assets .- data.net_assets_m1 .+ data.net_returns) ./
        data.net_assets_m1
    )
    return
end

function _windsorise_fund_flows!(data)
    flow_lowerbound = quantile(skipmissing(data.flow), 0.01)
    flow_upperbound = quantile(skipmissing(data.flow), 0.99)

    data[coalesce.(data.flow .< flow_lowerbound, false), :flow] .= flow_lowerbound
    data[coalesce.(data.flow .> flow_upperbound, false), :flow] .= flow_upperbound
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

function _calculate_riskfree(market_returns)
    usa_market = market_returns[market_returns.excntry .== "USA", :]
    usa_market.rf = usa_market.mkt_vw - usa_market.mkt_vw_exc

    rename!(usa_market, :eom => :date)
    usa_market.date = firstdayofmonth.(usa_market.date)

    select!(usa_market, [:date, :rf])
    return usa_market
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = process_mf_data()
    output_filename = makepath(DIRS.mf.refined, "mf-data.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing processed mutual fund data", task_start, minutes=false)
end