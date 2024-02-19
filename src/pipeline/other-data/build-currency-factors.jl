using Revise
using DataFrames
using Arrow
using Dates
using Statistics
using ShiftedArrays: lead, lag

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")
using .CommonFunctions
using .CommonConstants

const BASKET_ALLOCATION_ORDER = Dict(
    # Quantile number => Order that that quantile receives a currency added to the sample
    1 => 2,
    2 => 4,
    3 => 6,
    4 => 5,
    5 => 3,
    6 => 1
)

function build_currency_factors()
    task_start = time()

    filename = joinpath(DIRS.fx.refined, "currency_data.arrow")

    rates = Arrow.Table(filename) |> DataFrame
    
    _compute_delta_spot!(rates)
    _compute_forward_discount!(rates)
    _compute_carry_returns!(rates)

    _compute_interest_rate_ranking!(rates)
    _assign_interest_rate_baskets!(rates)
    _combine_net_carry_returns!(rates)

    basket_rates = _aggregate_baskets(rates)

    currency_factors = _compute_factors(basket_rates)

    currency_factors.date = firstdayofmonth.(currency_factors.date)
    stacked_factors = stack(
        currency_factors, Not(:date);
        variable_name=:factor, value_name=:ret
    )

    stacked_factors.ret ./= 100

    printtime("building currency factors", task_start, minutes=false)
    return stacked_factors
end

function _compute_delta_spot!(df)
    delta_spot_computation(spot) = log.(spot) - log.(lag(spot))

    transform!(
        groupby(df, :cur_code),
        :spot_mid => delta_spot_computation => :delta_spot
    )
end

function _compute_forward_discount!(df)
    function forward_discount_computation(spot, forward)
        missing_filter = 0*spot + 0*forward
        forward_discount = log.(lag(forward)) - log.(lag(spot)) + missing_filter

        return forward_discount
    end

    transform!(
        groupby(df, :cur_code),
        [:spot_mid, :forward_mid] => forward_discount_computation => :forward_discount
    )
end

function _compute_carry_returns!(df)
    input = [
        :delta_spot,
        :forward_discount,
        :forward_bid,
        :forward_ask,
        :spot_bid,
        :spot_ask
    ]
    output = [:carry_return, :net_long_carry_return, :net_short_carry_return]

    gross_carry(forward_discount, delta_spot) = forward_discount - delta_spot
    long_net_carry(forward_bid, spot_ask) = log.(lag(forward_bid)) - log.(spot_ask)
    short_net_carry(forward_ask, spot_bid) = -log.(lag(forward_ask)) + log.(spot_bid)

    carry_return_computations(Δs, d, fb, fa, sb, sa) = zip(
        gross_carry(d, Δs), long_net_carry(fb, sa), short_net_carry(fa, sb)
    )

    transform!(
        groupby(df, :cur_code),
        input => carry_return_computations => output
    )
end

function _compute_interest_rate_ranking!(df)
    function interest_rate_rank_computation(forward_discount)
        permutation_index = sortperm(forward_discount)
        rank = Array{Union{Missing, Int}}(undef, length(forward_discount))

        for (i, j) in enumerate(permutation_index)
            ismissing(forward_discount[j]) ? (rank[j] = missing) : rank[j] = i
        end

        return rank
    end

    transform!(
        groupby(df, :date),
        :forward_discount => interest_rate_rank_computation => :interest_rate_rank
    )
end

function _assign_interest_rate_baskets!(df)
    function interest_rate_basket_definition(rank)
        nonmissing_rank = skipmissing(rank)
        isempty(nonmissing_rank) && return fill(missing, length(rank))
        num_currencies = maximum(nonmissing_rank)

        basket_size(i) = Int(ceil((num_currencies - (BASKET_ALLOCATION_ORDER[i]-1))/6))

        max_rank_per_basket = cumsum([basket_size(i) for i in 1:6])

        basket = _assign_basket_num.(rank, Ref(max_rank_per_basket))

        return basket
    end

    transform!(
        groupby(df, :date),
        :interest_rate_rank => interest_rate_basket_definition => :interest_rate_basket
    )
end

function _assign_basket_num(row_rank, max_rank_per_basket)
    ismissing(row_rank) && return missing
    basket_num = findfirst(>=(row_rank), max_rank_per_basket)
    return basket_num
end

function _combine_net_carry_returns!(df)
    df.net_carry_return = Vector{Union{Missing, Float64}}(missing, nrow(df))
    for row in eachrow(df)
        !ismissing(row.interest_rate_basket) || continue
        
        row.interest_rate_basket == 1 && (row.net_carry_return = row.net_short_carry_return)
        row.interest_rate_basket != 1 && (row.net_carry_return = row.net_long_carry_return)
    end
end

function _aggregate_baskets(df)
    basketed_df = df[.!ismissing.(df.interest_rate_basket), :]
    aggregated_columns = [:delta_spot, :forward_discount, :carry_return, :net_carry_return]

    basket_rates = combine(
        groupby(basketed_df, [:date, :interest_rate_basket]),
        aggregated_columns .=> mean .=> aggregated_columns
    )
    return basket_rates
end

function _compute_factors(df)
    input = [:delta_spot, :carry_return, :net_carry_return, :interest_rate_basket]
    output = [:rx, :hml_fx, :rx_net, :hml_fx_net, :dollar, :carry]

    function hml(basket_nums, series)
        basketed_series = Dict(
            basket => series for (basket, series) in zip(basket_nums, series)
        )

        return basketed_series[6] - basketed_series[1]
    end

    function factor_computations(delta_spot, carry_return, net_carry_return, basket_num)
        rx = mean(skipmissing(carry_return))
        hml_fx = hml(basket_num, carry_return)
        rx_net = mean(skipmissing(net_carry_return))
        hml_fx_net = hml(basket_num, net_carry_return)
        dollar = mean(skipmissing(delta_spot))
        carry = hml(basket_num, delta_spot)
        
        return [100 .* (rx, hml_fx, rx_net, hml_fx_net, dollar, carry)]
    end

    factors = combine(
        groupby(df, :date),
        input => factor_computations => output
    )

    return factors
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = build_currency_factors()
    output_filename = makepath(DIRS.fx.factors, "currency_factors.arrow")

    task_start = time()
    Arrow.write(output_filename, output_data)
    printtime("writing currency factors", task_start, minutes=false)
end