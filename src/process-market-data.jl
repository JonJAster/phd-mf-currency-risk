using
    DataFrames,
    CSV,
    Arrow

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using
    .CommonFunctions,
    .CommonConstants

const INPUT_DIR_MARKET = joinpath(DIRS.equity, "raw")
const INPUT_DIR_FX = joinpath(DIRS.currency, "combined")
const OUTPUT_DIR = joinpath(DIRS.equity, "factor-series")
const READ_COLUMNS_MARKET = [:excntry, :eom, :me_lag1, :mkt_vw]
const READ_COLUMNS_FX = [:cur_code, :date, :spot_mid, :forward_mid]

function main()
    time_start = time()

    println("Reading data...")
    filename_market = joinpath(INPUT_DIR_MARKET, "country_market_data.csv")
    filename_rf = joinpath(INPUT_DIR_MARKET, "usd_riskfree.csv")
    filename_fx = joinpath(INPUT_DIR_FX, "currency_rates_unfiltered.arrow")

    market_returns = CSV.read(filename_market, DataFrame, select=READ_COLUMNS_MARKET)
    usd_rf = CSV.read(filename_rf, DataFrame)
    fx_rates = Arrow.Table(filename_fx) |> DataFrame

    println("Aggregating to global...")
    world_index_usd = aggregate_by_market_equity(market_returns)

    println("Copying to local currencies...")
    world_index_local = build_local_indices(world_index_usd, fx_rates)

    println("Building local risk-free series...")
    local_rf = build_local_rf(usd_rf, fx_rates)

    global_market_data = innerjoin(world_index_local, local_rf, on=[:cur_code, :date])
    global_market_data.mkt = global_market_data.mkt_gross .- global_market_data.rf

    output_filestring = makepath(OUTPUT_DIR, "global_mkt_data.arrow")

    Arrow.write(output_filestring, global_market_data)

    time_duration = round(time() - time_start, digits=2)
    println("Finished processing market data in $time_duration seconds")
end

function aggregate_by_market_equity(df)
    df_by_date = groupby(df, :eom) |> x->combine(x, :me_lag1 => sum => :me_lag1_total)
    df = innerjoin(df, df_by_date, on=:eom)
    df.weight = df.me_lag1 ./ df.me_lag1_total
    df.weighted_mkt_vw = df.weight .* df.mkt_vw

    output = groupby(df, :eom) |> x->combine(x, :weighted_mkt_vw => sum => :mkt_vw)
    output = output[:, [:eom, :mkt_vw]]
    rename!(output, [:eom => :date, :mkt_vw => :mkt_gross])
    sort!(output, :date)
    return output
end

function build_local_indices(market_data, rates_data)
    currency_list = unique(rates_data.cur_code)

    local_market_data_set = DataFrame[]
    push_with_currency_code!(local_market_data_set, market_data, "USD", :mkt_gross)

    for currency in currency_list
        rates = rates_data[rates_data.cur_code .== currency, :]
        country_market_data = innerjoin(market_data, rates, on=:date)
        country_market_data.mkt_gross = country_market_data.mkt_gross .* country_market_data.spot_mid

        push_with_currency_code!(local_market_data_set, country_market_data, currency, :mkt_gross)
    end

    output = vcat(local_market_data_set...)
    sort!(output, [:cur_code, :date])
    return output
end

function build_local_rf(usd_rf, rates_data)
    currency_list = unique(rates_data.cur_code)

    local_riskfree_set = DataFrame[]
    push_with_currency_code!(local_riskfree_set, usd_rf, "USD", :rf)

    for currency in currency_list
        currency_riskfree = calculate_riskfree_by_cip(usd_rf, rates_data, currency)
        push_with_currency_code!(local_riskfree_set, currency_riskfree, currency, :rf)
    end

    output = vcat(local_riskfree_set...)
    sort!(output, [:cur_code, :date])
    dropmissing!(output, :rf)
    return output
end

function calculate_riskfree_by_cip(base_rf, rates_data, currency_code)
    country_rates = rates_data[rates_data.cur_code .== currency_code, :]

    country_riskfree = innerjoin(country_rates, base_rf, on=:date)
    country_riskfree.rf = (
        country_riskfree.rf ./ country_riskfree.spot_mid .* country_riskfree.forward_mid
    )

    return country_riskfree
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end