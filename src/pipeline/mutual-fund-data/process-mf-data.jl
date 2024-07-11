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
    data = loadarrow(data_filename)
    info = loadarrow(info_filename)

    # Combine fund class and fund class group ids into a unique identifier at the fund level
    _identify_funds!(data)

    _add_load_dummies!(data)

    aggregate_data = _aggregate_to_fund_level(data)
    _calculate_fund_flows!(aggregate_data) # TODO: Verify calculation
    _clip_fund_flows!(aggregate_data) # TODO: Verify the accuracy and necessity of this step

    # TODO: There are only 161 rows with all non-missing values - what happened?
    dropmissing(aggregate_data)


    #sort!(data, [:class_group_id, :fund_class_id, :date])

        # aggregate_data = _aggregate_to_fundid(active_data)

        # _null_out_small!(aggregate_data)
        # _trim_missing_tails!(aggregate_data)
        # _calculate_fund_flows!(aggregate_data)
        # _clip_fund_flows!(aggregate_data)
        # _filter_out_low_obs_funds!(aggregate_data)

        # rename!(aggregate_data, :net_returns => :ret)
        # aggregate_data[:, [:ret, :costs]] ./= 100

        # output = select(
        #     aggregate_data, 
        #     [:fundid, :date, :flow, :ret, :costs, :net_assets_m1]
        # )

    printtime("processing mutual fund data", task_start, minutes=false)
    return
end

function _identify_funds!(data)
    # Class group IDs are supposed all be 2_xxx_xxx, but some are smaller integers.
    # In the original dataset it is verified that adding 2_000_000 to the smaller integers
    # does not result in any collisions with the larger integers, but it is checked
    # here for safety of future data updates.
    shift_collisions = intersect(
        Set(skipmissing(data.class_group_id)).+ 2_000_000,
        skipmissing(data.class_group_id)
    )
    
    if !isempty(shift_collisions)
        # TODO: Reinstate this error once fixed
        # error("Class group IDs collide after shifting by 2_000_000")
    end

    # TODO: Clean this up by finding a better unique identifier
    data[coalesce.(data.class_group_id .< 2_000_000, false), :class_group_id] .= (
            -1 .* (
                data[coalesce.(data.class_group_id .< 2_000_000, false), :class_group_id] 
                .+ 2_000_000
            )
    )

    # Use the class group ID if it exists, otherwise use the only class's fund class ID
    data.fund_id = coalesce.(data.class_group_id, data.fund_class_id)

    select!(data, :fund_id, Not(:fund_id))

    return data.fund_id
end

function _add_load_dummies!(data)
    data.pure_no_load = (data.front_load .== 0) .& (data.rear_load .== 0)
    data.morningstar_no_load = data.pure_no_load .& coalesce.(
            data.actual_12b1 .<= 0.0025,
            data.max_12b1 .<= 0.0025
    )

    return data[!, [:pure_no_load, :morningstar_no_load]]
end

function _aggregate_to_fund_level(multi_class_funds)
    # TODO: Test if this aggregates properly, especially the weight lagging
    single_class_funds = multi_class_funds[ismissing.(multi_class_funds.class_group_id), :]
    multi_class_funds = multi_class_funds[.!ismissing.(multi_class_funds.class_group_id), :]

    # Sorting by date is sufficient to ensure correct lagging within groups
    sort!(multi_class_funds, :date)

    total_assets = combine(
        groupby(multi_class_funds, [:fund_id, :date]),
        :net_assets => sum => :total_net_assets
    )

    data_total = innerjoin(multi_class_funds, total_assets, on=[:fund_id, :date])
    data_total.lead_weight = data_total.net_assets ./ data_total.total_net_assets
    data_weighted = transform(
        groupby(data_total, :fund_class_id),
        :lead_weight => lag => :weight
    )
    
    data_weighted.weighted_ret = data_weighted.weight .* data_weighted.ret
    data_weighted.weighted_costs = data_weighted.weight .* data_weighted.costs

    aggregate_data = combine(
        groupby(data_weighted, [:fund_id, :date]),
        :net_assets => sum => :net_assets,
        :weighted_ret => sum => :ret,
        :weighted_costs => sum => :costs
    )

    transform!(
        groupby(aggregate_data, :fund_id),
        :net_assets => lag => :net_assets_m1
    )

    # Aggregate is already date sorted
    sort!(aggregate_data, :fund_id)

    return aggregate_data
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
    data.flow = (data.net_assets ./ data.net_assets_m1) .- (1 .+ (data.ret ./ 100))
    return
end

function _clip_fund_flows!(data)
    flow_lowerbound = -0.9
    flow_upperbound = 10

    data[
        coalesce.(data.flow .<= flow_lowerbound,false) .||
        coalesce.(data.flow .>= flow_upperbound,false),
        [:net_assets, :ret, :costs, :net_assets_m1, :flow]
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

if abspath(PROGRAM_FILE) == @__FILE__
    process_mf_data()
end