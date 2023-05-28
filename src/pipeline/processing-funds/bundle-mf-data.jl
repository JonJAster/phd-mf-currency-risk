using
    DataFrames,
    Arrow,
    CSV,
    Base.Threads

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using
    .CommonFunctions,
    .CommonConstants

const INPUT_DIR = joinpath(DIRS.fund, "post-processing")
const OUTPUT_DIR = INPUT_DIR

const READ_COLUMNS = [
    :fundid, :date, :ret_gross_m, :mean_costs, :fund_assets, :fund_flow, :domicile
]
const OUTPUT_COLUMNS = [
    :fundid, :date, :currency, :ret, :fund_flow, :domicile, :fund_assets, :mean_costs
]

function main(options_folder=option_foldername(; DEFAULT_OPTIONS...))
    time_start = time()
    println("Reading data...")
    filename_map = joinpath(DIRS.map, "currency_to_country.csv")
    
    fund_data = load_fund_data(options_folder)
    currency_map = CSV.read(filename_map, DataFrame)

    println("Mapping countries to currencies...")
    fund_data.currency = map_currency(fund_data.date, fund_data.domicile, currency_map)

    rename!(fund_data, :ret_gross_m => :ret)
    select!(fund_data, OUTPUT_COLUMNS)

    fund_data.ret = fund_data.ret / 100

    output_filestring = makepath(OUTPUT_DIR, options_folder, "main/fund_data.arrow")
    Arrow.write(output_filestring, fund_data)

    duration_s = round(time() - time_start, digits=2)
    println("Finished bundling mutual fund data in $duration_s seconds.")
end

function load_fund_data(options_folder)
    dirstring = joinpath(INPUT_DIR, options_folder, "initialised")
    output_data = load_data_in_parts(dirstring, select=READ_COLUMNS)
    return output_data
end

function map_currency(date_series, country_series, currency_map)
    currency_series = Vector{Union{Missing, String}}(missing, length(date_series))

    @threads for i in eachindex(date_series)
        date = date_series[i]
        country = country_series[i]

        country_match = currency_map.country_code .== country
        date_match = notoutside.(Ref(date), currency_map.start_date, currency_map.end_date)

        match_rows = currency_map[country_match .& date_match, :]

        nrow(match_rows) > 1 && println("Warning: multiple matches for $country on $date")
        nrow(match_rows) > 0 && (currency_series[i] = match_rows[1, :currency_code])
    end

    return currency_series
end

function notoutside(date, start_date, end_date)
    !ismissing(start_date) && date < start_date && return false
    !ismissing(end_date) && date > end_date && return false
    return true
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end