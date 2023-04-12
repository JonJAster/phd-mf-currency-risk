using CSV
using DataFrames
using Statistics
using ShiftedArrays: lead, lag

const INPUT_FILESTRING = "./data/prepared/currencies/currency_rates.csv"
const OUTPUT_FILESTRING_BASE = "./data/transformed/currencies"

const BASKET_ALLOCATION_ORDER = Dict(
    # Quantile number => Order that that quantile receives a currency added to the sample
    1 => 2,
    2 => 4,
    3 => 6,
    4 => 5,
    5 => 3,
    6 => 1
)

function group_transform!(df, group_cols, input_cols, f::Function, output_cols)
    groupby(df, group_cols) |> x -> transform!(x, input_cols => f => output_cols)
end

function group_combine(df, group_cols, input_cols, f::Function, output_cols; cast=true)
    if cast
        groupby(df, group_cols) |> x -> combine(x, input_cols .=> f .=> output_cols)
    else
        groupby(df, group_cols) |> x -> combine(x, input_cols => f => output_cols)
    end
end

function compute_delta_spot!(df)
    group_on = :cur_code
    input = :spot_mid
    output = :delta_spot

    delta_spot_computation(spot) = log.(spot) - log.(lag(spot))

    group_transform!(df, group_on, input, delta_spot_computation, output)
end

function compute_forward_discount!(df)
    group_on = :cur_code
    input = [:spot_mid, :forward_mid]
    output = :forward_discount

    function forward_discount_computation(spot, forward)
        missing_filter = 0*spot + 0*forward
        forward_discount = log.(lag(forward)) - log.(lag(spot)) + missing_filter

        return forward_discount
    end

    group_transform!(df, group_on, input, forward_discount_computation, output)
end

function compute_carry_returns!(df)
    group_on = :cur_code
    input = [
        :delta_spot, :forward_discount, :forward_bid, :forward_ask, :spot_bid, :spot_ask
    ]
    output = [:carry_return, :net_long_carry_return, :net_short_carry_return]

    gross_carry(forward_discount, delta_spot) = forward_discount - delta_spot
    long_net_carry(forward_bid, spot_ask) = log.(lag(forward_bid)) - log.(spot_ask)
    short_net_carry(forward_ask, spot_bid) = -log.(lag(forward_ask)) + log.(spot_bid)

    carry_return_computations(Δs, d, fb, fa, sb, sa) = zip(
        gross_carry(d, Δs), long_net_carry(fb, sa), short_net_carry(fa, sb)
    )

    group_transform!(df, group_on, input, carry_return_computations, output)
end

function compute_interest_rate_ranking!(df)
    group_on = :date
    input = :forward_discount
    output = :interest_rate_rank

    function interest_rate_rank_computation(forward_discount)
        permutation_index = sortperm(forward_discount)
        rank = Array{Union{Missing, Int}}(undef, length(forward_discount))

        for (i, j) in enumerate(permutation_index)
            ismissing(forward_discount[j]) ? (rank[j] = missing) : rank[j] = i
        end

        return rank
    end

    group_transform!(df, group_on, input, interest_rate_rank_computation, output)
end

function assign_interest_rate_baskets!(df)
    group_on = :date
    input = :interest_rate_rank
    output = :interest_rate_basket

    function interest_rate_basket_definition(rank)
        nonmissing_rank = skipmissing(rank)
        isempty(nonmissing_rank) && return fill(missing, length(rank))
        num_currencies = maximum(nonmissing_rank)

        basket_size(i) = Int(ceil((num_currencies - (BASKET_ALLOCATION_ORDER[i]-1))/6))

        max_rank_per_basket = cumsum([basket_size(i) for i in 1:6])

        basket = assign_basket_num.(rank, Ref(max_rank_per_basket))

        return basket
    end

    group_transform!(df, group_on, input, interest_rate_basket_definition, output)
end

function assign_basket_num(row_rank, max_rank_per_basket)
    ismissing(row_rank) && return missing
    basket_num = findfirst(>=(row_rank), max_rank_per_basket)
    return basket_num
end

function combine_net_carry_returns!(df)
    df.net_carry_return = Vector{Union{Missing, Float64}}(missing, nrow(df))
    for row in eachrow(df)
        !ismissing(row.interest_rate_basket) || continue
        
        row.interest_rate_basket == 1 && (row.net_carry_return = row.net_short_carry_return)
        row.interest_rate_basket != 1 && (row.net_carry_return = row.net_long_carry_return)
    end
end

function aggregate_baskets(df)
    basketed_df = df[.!ismissing.(df.interest_rate_basket), :]

    group_on = [:date, :interest_rate_basket]
    aggregated_columns = [:delta_spot, :forward_discount, :carry_return, :net_carry_return]

    basket_rates = group_combine(
        basketed_df, group_on, aggregated_columns, mean, aggregated_columns
    )

    sort!(basket_rates, [:date, :interest_rate_basket])

    return basket_rates
end

function compute_factors(df)
    group_on = :date
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

    factors = group_combine(df, group_on, input, factor_computations, output, cast=false)

    return factors
end
    

function main()
    rates = CSV.read(INPUT_FILESTRING, DataFrame)
    
    compute_delta_spot!(rates)
    compute_forward_discount!(rates)
    compute_carry_returns!(rates)

    compute_interest_rate_ranking!(rates)
    assign_interest_rate_baskets!(rates)
    combine_net_carry_returns!(rates)

    basket_rates = aggregate_baskets(rates)

    currency_factors = compute_factors(basket_rates)

    if !isdir(OUTPUT_FILESTRING_BASE)
        mkpath(OUTPUT_FILESTRING_BASE)
    end

    OUTPUT_FILESTRING = joinpath(OUTPUT_FILESTRING_BASE, "currency_factors.csv")
    CSV.write(OUTPUT_FILESTRING, currency_factors)
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end