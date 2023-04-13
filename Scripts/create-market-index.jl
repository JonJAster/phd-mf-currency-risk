using DataFrames
using CSV

const INPUT_FILESTRING_MARKET = "./data/raw/equities/country_market_data.csv"
const INPUT_FILESTRING_FX = "./data/prepared/currencies/currency_rates.csv"
const OUTPUT_FILESTRING_BASE = "./data/transformed/equities"
const READ_COLUMNS_MARKET = [:excntry, :eom, :me_lag1, :mkt_vw]
const READ_COLUMNS_FX = [:cur_code, :date, :spot_mid]

function aggregate_by_market_equity(df)
    df_by_date = groupby(df, :eom) |> x->combine(x, :me_lag1 => sum => :me_lag1_total)
    df = innerjoin(df, df_by_date, on=:eom)
    df.weight = df.me_lag1 ./ df.me_lag1_total
    df.weighted_mkt_vw = df.weight .* df.mkt_vw

    output = groupby(df, :eom) |> x->combine(x, :weighted_mkt_vw => sum => :mkt_vw)
    output = output[:, [:eom, :mkt_vw]]
    rename!(output, [:eom => :date, :mkt_vw => :mkt])
    sort!(output, :date)
    return output
end

function build_local_indices(market_data, rates_data)
    currency_list = unique(rates_data.cur_code)

    translated_market_data = DataFrame[]
    push_with_currency_code!(translated_market_data, market_data, "USD")

    for currency in currency_list
        rates = rates_data[rates_data.cur_code .== currency, :]
        country_market_data = innerjoin(market_data, rates, on=:date)
        country_market_data.mkt = country_market_data.mkt .* country_market_data.spot_mid

        push_with_currency_code!(translated_market_data, country_market_data, currency)
    end

    output = vcat(translated_market_data...)
    sort!(output, [:cur_code, :date])
    return output
end

function push_with_currency_code!(datalist, df, currency_code)
    append_data = copy(df)
    append_data.cur_code .= currency_code
    append_data = append_data[:, [:cur_code, :date, :mkt]]
    push!(datalist, append_data)
end

function main()
    market_data = CSV.read(INPUT_FILESTRING_MARKET, DataFrame, select=READ_COLUMNS_MARKET)
    fx_rates = CSV.read(INPUT_FILESTRING_FX, DataFrame, select=READ_COLUMNS_FX)

    world_index_usd = aggregate_by_market_equity(market_data)

    world_index_local = build_local_indices(world_index_usd, fx_rates)

    if !isdir(OUTPUT_FILESTRING_BASE)
        mkpath(OUTPUT_FILESTRING_BASE)
    end

    OUTPUT_FILESTRING = joinpath(OUTPUT_FILESTRING_BASE, "global_MKT.csv")

    CSV.write(OUTPUT_FILESTRING, world_index_local)
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end