using DataFrames
using Arrow
using StatsBase
using BenchmarkTools
using DataFramesMeta
using Plots
using GLM
using Dates

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
include("shared/DataInit.jl")
using
    .CommonFunctions,
    .CommonConstants,
    .DataInit

options_folder = option_foldername(; DEFAULT_OPTIONS...)
main_folder = joinpath(DIRS.fund, "post-processing", options_folder, "initialised")
main_data = load_data_in_parts(main_folder)

info = load_data_in_parts(joinpath(DIRS.fund, "domicile-grouped/info"))

halffilt = info[findall(x->!isnothing(match(r"Vanguard.*", x)), info."fund-class-name"),:]
filt = halffilt[nonmissing(halffilt.domicile .== "United States"), :]

x = filt[findall(x->!isnothing(match(r".*International .*", x)), filt."fund-class-name"),:]
x[4:5,7]

names(info)[6]
info[nonmissing(info.fundid .== "FSUSA002L7"), 7]

test_fund = main_data[main_data.fundid .== "FSUSA002L7",:]
test_fund[abs.(test_fund.date - Dates.Date(2020, 3, 31)) .<= Dates.Day(100), :]

eq = Arrow.Table(joinpath(DIRS.equity, "factor-series/equity_factors.arrow")) |> DataFrame
mkt = Arrow.Table(joinpath(DIRS.equity, "factor-series/unhedged_global_mkt.arrow")) |> DataFrame
cf = Arrow.Table(joinpath(DIRS.currency, "factor-series/currency_factors.arrow")) |> DataFrame

date_range = Set(Dates.lastdayofmonth(Dates.Date(2020, 01, 31) + Dates.Month(i)) for i in 0:5)

eq[eq.date .∈ Ref(date_range),:]
mkt[(mkt.currency .== "USD") .&& (mkt.date .∈ Ref(date_range)),:]
cf[cf.date .∈ Ref(date_range),:]

betas = Arrow.Table(joinpath(DIRS.fund, "post-processing", options_folder, "factor-betas/world_capm_verdelhan.arrow")) |> DataFrame
betas[betas.date .∈ Ref(date_range),:]

# const INPUT_DIR = joinpath(DIRS.currency, "combined")
# const OUTPUT_DIR = joinpath(DIRS.currency, "factor-series")

# function main()
#     time_start = time()
#     println("Loading currency rates...")
#     filename = joinpath(INPUT_DIR, "currency_rates.arrow")

#     rates = Arrow.Table(filename) |> DataFrame
    
#     println("Computing currency factors...")
#     compute_delta_spot!(rates)
#     compute_forward_discount!(rates)
#     compute_carry_returns!(rates)

#     compute_interest_rate_ranking!(rates)
#     assign_interest_rate_baskets!(rates)
#     combine_net_carry_returns!(rates)

#     basket_rates = aggregate_baskets(rates)

#     currency_factors = compute_factors(basket_rates)

#     output_filestring = makepath(OUTPUT_DIR, "currency_factors.arrow")

#     Arrow.write(output_filestring, currency_factors)

#     duration_s = round(time() - time_start, digits=2)

#     println("Finished computing currency factors in $duration_s seconds ")
# end

# basket_rates_pivot = unstack(basket_rates, :date, :interest_rate_basket, :carry_return)
# findfirst(==(Dates.Date(2020,05,31)), basket_rates_pivot.date)
# println(basket_rates_pivot[439:(439+6), :])


# const BASKET_ALLOCATION_ORDER = Dict(
#     # Quantile number => Order that that quantile receives a currency added to the sample
#     1 => 2,
#     2 => 4,
#     3 => 6,
#     4 => 5,
#     5 => 3,
#     6 => 1
# )

# function compute_delta_spot!(df)
#     group_on = :cur_code
#     input = :spot_mid
#     output = :delta_spot

#     delta_spot_computation(spot) = log.(spot) - log.(lag(spot))

#     group_transform!(df, group_on, input, delta_spot_computation, output)
# end

# function compute_forward_discount!(df)
#     group_on = :cur_code
#     input = [:spot_mid, :forward_mid]
#     output = :forward_discount

#     function forward_discount_computation(spot, forward)
#         missing_filter = 0*spot + 0*forward
#         forward_discount = log.(lag(forward)) - log.(lag(spot)) + missing_filter

#         return forward_discount
#     end

#     group_transform!(df, group_on, input, forward_discount_computation, output)
# end

# function compute_carry_returns!(df)
#     group_on = :cur_code
#     input = [
#         :delta_spot, :forward_discount, :forward_bid, :forward_ask, :spot_bid, :spot_ask
#     ]
#     output = [:carry_return, :net_long_carry_return, :net_short_carry_return]

#     gross_carry(forward_discount, delta_spot) = forward_discount - delta_spot
#     long_net_carry(forward_bid, spot_ask) = log.(lag(forward_bid)) - log.(spot_ask)
#     short_net_carry(forward_ask, spot_bid) = -log.(lag(forward_ask)) + log.(spot_bid)

#     carry_return_computations(Δs, d, fb, fa, sb, sa) = zip(
#         gross_carry(d, Δs), long_net_carry(fb, sa), short_net_carry(fa, sb)
#     )

#     group_transform!(df, group_on, input, carry_return_computations, output)
# end

# function compute_interest_rate_ranking!(df)
#     group_on = :date
#     input = :forward_discount
#     output = :interest_rate_rank

#     function interest_rate_rank_computation(forward_discount)
#         permutation_index = sortperm(forward_discount)
#         rank = Array{Union{Missing, Int}}(undef, length(forward_discount))

#         for (i, j) in enumerate(permutation_index)
#             ismissing(forward_discount[j]) ? (rank[j] = missing) : rank[j] = i
#         end

#         return rank
#     end

#     group_transform!(df, group_on, input, interest_rate_rank_computation, output)
# end

# function assign_interest_rate_baskets!(df)
#     group_on = :date
#     input = :interest_rate_rank
#     output = :interest_rate_basket

#     function interest_rate_basket_definition(rank)
#         nonmissing_rank = skipmissing(rank)
#         isempty(nonmissing_rank) && return fill(missing, length(rank))
#         num_currencies = maximum(nonmissing_rank)

#         basket_size(i) = Int(ceil((num_currencies - (BASKET_ALLOCATION_ORDER[i]-1))/6))

#         max_rank_per_basket = cumsum([basket_size(i) for i in 1:6])

#         basket = assign_basket_num.(rank, Ref(max_rank_per_basket))

#         return basket
#     end

#     group_transform!(df, group_on, input, interest_rate_basket_definition, output)
# end

# function assign_basket_num(row_rank, max_rank_per_basket)
#     ismissing(row_rank) && return missing
#     basket_num = findfirst(>=(row_rank), max_rank_per_basket)
#     return basket_num
# end

# function combine_net_carry_returns!(df)
#     df.net_carry_return = Vector{Union{Missing, Float64}}(missing, nrow(df))
#     for row in eachrow(df)
#         !ismissing(row.interest_rate_basket) || continue
        
#         row.interest_rate_basket == 1 && (row.net_carry_return = row.net_short_carry_return)
#         row.interest_rate_basket != 1 && (row.net_carry_return = row.net_long_carry_return)
#     end
# end

# function aggregate_baskets(df)
#     basketed_df = df[.!ismissing.(df.interest_rate_basket), :]

#     group_on = [:date, :interest_rate_basket]
#     aggregated_columns = [:delta_spot, :forward_discount, :carry_return, :net_carry_return]

#     basket_rates = group_combine(
#         basketed_df, group_on, aggregated_columns, mean, aggregated_columns
#     )

#     sort!(basket_rates, [:date, :interest_rate_basket])

#     return basket_rates
# end

# function compute_factors(df)
#     group_on = :date
#     input = [:delta_spot, :carry_return, :net_carry_return, :interest_rate_basket]
#     output = [:rx, :hml_fx, :rx_net, :hml_fx_net, :dollar, :carry]

#     function hml(basket_nums, series)
#         basketed_series = Dict(
#             basket => series for (basket, series) in zip(basket_nums, series)
#         )

#         return basketed_series[6] - basketed_series[1]
#     end

#     function factor_computations(delta_spot, carry_return, net_carry_return, basket_num)
#         rx = mean(skipmissing(carry_return))
#         hml_fx = hml(basket_num, carry_return)
#         rx_net = mean(skipmissing(net_carry_return))
#         hml_fx_net = hml(basket_num, net_carry_return)
#         dollar = mean(skipmissing(delta_spot))
#         carry = hml(basket_num, delta_spot)
        
#         return [100 .* (rx, hml_fx, rx_net, hml_fx_net, dollar, carry)]
#     end

#     factors = group_combine(df, group_on, input, factor_computations, output, cast=false)

#     return factors
# end

# rates = Arrow.Table(
#     joinpath(DIRS.currency, "combined", "currency_rates.arrow")
# ) |> DataFrame

# rates_pivot = unstack(rates, :date, :cur_code, :spot_mid)

# println(rates_pivot[300:306, [1, 4, 6, 7, 12, 15]])

# x = rates_pivot.AUD
# y = rates_pivot.EUR

# fit = lm(@formula(EUR ~ AUD), rates_pivot)
# println(coeftable(fit))

# c = coef(fit)[1]
# m = coef(fit)[2]

# minx = minimum(skipmissing(x))
# maxx = maximum(skipmissing(x))
# miny = minimum(skipmissing(y))
# maxy = maximum(skipmissing(y))

# line = DataFrame(
#     AUD = 1/100*(101*minx:98*maxx),
#     EUR = c .+ m .* 1/100*(101*minx:98*maxx)
# )

# left_cross_x = 1.33
# right_cross_x = 1.5
# desired_cross_x = 1.4
# desired_cross_y = c + m*desired_cross_x
# perpendicular_c = desired_cross_y + 1/m*1.4

# perpendicular_line = DataFrame(
#     AUD = 1/100*(100*left_cross_x:100*right_cross_x),
#     EUR = perpendicular_c .- 1/m .* 1/100*(100*left_cross_x:100*right_cross_x)
# )

# width_ratio = (maxx - minx)/(maxy - miny)

# plotheight = 300
# plot(
#     x, y, seriestype=:scatter,
#     markersize=1.5, markerstrokewidth=0,
#     legend=:none, xlabel="AUD", ylabel="EUR",
#     title="AUD/USD vs EUR/USD",
#     size=(width_ratio*plotheight, plotheight)
# )

# plot!(line.AUD, line.EUR, linewidth=1, color=:black, linestyle=:dash, label="Regression Line")
# plot!(perpendicular_line.AUD, perpendicular_line.EUR, linewidth=1, color=:black, linestyle=:dash, label="Perpendicular Line")

## UNUSED TEST

# options_folder1 = option_foldername()
# options_folder2 = option_foldername(; DEFAULT_OPTIONS...)

# data1 = Arrow.Table(
#     joinpath(DIRS.fund, "post-processing", options_folder, "main/fund_data.arrow")
# ) |> DataFrame

# data2 = Arrow.Table(
#     joinpath(DIRS.fund, "post-processing", options_folder, "main/fund_data.arrow")
# ) |> DataFrame

## END UNUSED

# dftest = initialise_base_data(option_foldername(;DEFAULT_OPTIONS...))
# filtered_df = infofilter(:broad_category => x-> nonmissing(x!="Equity"), dftest)

# infolookup("FSUSA0BGZ0")

# categ = load_data_in_parts("data/mutual-funds/raw/monthly-morningstar-category")
# categ_lookup = categ[categ.FundId .== "FSUSA0BGZ0", :]


# info_filename = joinpath(DIRS.fund, "info", "mf_info.arrow")
# fund_info = Arrow.Table(info_filename) |> DataFrame

# filter_info = filter(:broad_category => x-> nonmissing(x!="equity"), fund_info)
# filtered_ids = filter_info.fundid |> Set

# "FS00008KO2" ∈ filtered_ids
# f2 = filter(:fundid => in(filtered_ids), dftest)

# filter(:FundId=>x->(x!=""&&nonmissing(x[1:9]=="FS00008K0")), info)

# info = load_data_in_parts("data/mutual-funds/raw/info")

# testfolders = ["local-monthly-gross-returns", "local-monthly-net-returns",
#                "monthly-costs", "monthly-morningstar-category", "monthly-net-assets",
#                "usd-monthly-gross-returns", "usd-monthly-net-returns"]

# info_ids = Set(info.FundId)
# for folder in testfolders
#     testdata = load_data_in_parts("data/mutual-funds/raw/$folder", select=[:FundId])
#     test_ids = Set(testdata.FundId)
#     n_all = length(union(test_ids, info_ids))
#     n_data_ex = length(setdiff(test_ids, info_ids))
#     n_info_ex  = length(setdiff(info_ids, test_ids))
#     n_datainfo_ix = length(intersect(test_ids, info_ids))
#     println(
#         "$folder: There are $n_all funds in total, which are $n_data_ex data-only " *
#         "funds, $n_info_ex info-only funds, and $n_datainfo_ix funds with both data and " *
#         "info."
#     )
# end

# testdf = DataFrame(a=[1,2,3,4,5], b=[1,2,3,4,5], c=[1,2,3,4,5], d=[1,2,3,4,5], e=[1,2,3,4,5])

# filter([:a, :b]=>(x,y)->(x==1 || y==2), testdf)