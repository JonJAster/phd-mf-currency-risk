using
    DataFrames,
    CSV,
    Arrow,
    Dates

using DataStructures: OrderedDict

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using
    .CommonConstants,
    .CommonFunctions

const
    INPUT_DIR = joinpath(DIRS.currency, "raw")
    OUTPUT_DIR = joinpath(DIRS.currency, "combined")

const
    RATE_SETTLEMENTS = ["spot", "forward"]
    RATE_LEVELS = ["bid", "mid", "ask"]
    INVERTED_RATE_LEVELS = Dict("bid" => "ask", "mid" => "mid", "ask" => "bid")
    RATE_TYPES =  Iterators.product(RATE_SETTLEMENTS, RATE_LEVELS)
    RATE_TYPE_NAMES = (vec ∘ collect)("$(a)_$b" for (a,b) in RATE_TYPES)
    DATE_FORMAT = DateFormat("dd/mm/yyyy")

const 
    RateType = Tuple{String, String}
    RateSet = Vector{Vector{Union{Missing,Float64}}}

const CIP_VIOLATIONS = [
    (currency="AED", start_date="2006-06-30", end_date="2006-11-30"),
    (currency="IDR", start_date="2000-12-31", end_date="2007-05-31"),
    (currency="MYR", start_date="1998-08-31", end_date="2005-06-30"),
    (currency="TRY", start_date="2000-10-31", end_date="2001-11-30"),
    (currency="ZAR", start_date="1985-07-31", end_date="1985-08-31")
]
const EURO_CONSTITUENTS = [
    (currency="ATS", start_date="1999-01-01"),
    (currency="BEF", start_date="1999-01-01"),
    (currency="DEM", start_date="1999-01-01"),
    (currency="ESP", start_date="1999-01-01"),
    (currency="FIM", start_date="1999-01-01"),
    (currency="FRF", start_date="1999-01-01"),
    (currency="IEP", start_date="1999-01-01"),
    (currency="ITL", start_date="1999-01-01"),
    (currency="NGL", start_date="1999-01-01"),
    (currency="PTE", start_date="1999-01-01"),
    (currency="GRD", start_date="2001-01-01"),
]

function main()
    time_start = time()
    
    println("Reading currency info...")
    filestring_info = joinpath(INPUT_DIR, "currency_info.csv")
    info = CSV.read(filestring_info, DataFrame)
    
    println("Reading raw rate data...")
    rate_data = Dict{RateType, DataFrame}()
    for (a, b) in RATE_TYPES
        filestring = joinpath(INPUT_DIR, "$(a)_$b.csv")
        rate_data[(a, b)] = CSV.read(
            filestring, DataFrame, missingstring="NA",
            dateformat=DATE_FORMAT, types=Dict(:date => Date)
        )
    end

    assert_equal_dates!(rate_data)

    currency_series = DataFrame[]
    unique_currencies = unique(info[:, :cur_code])

    println("Processing rate data...")
    for i in unique_currencies
        push!(currency_series, build_rate_series(i, info, rate_data))
    end

    currency_table = vcat(currency_series...)
    currency_table[!, :date] = lastdayofmonth.(currency_table[!, :date])

    dropmissing!(currency_table, RATE_TYPE_NAMES, disallowmissing=false)
    end_euro_constituents!(currency_table)

    currency_table_cip_violated = copy(currency_table)
    remove_cip_violations!(currency_table)

    sort!(currency_table, [:cur_code, :date])
    sort!(currency_table_cip_violated, [:cur_code, :date])

    output_filestring = makepath(OUTPUT_DIR, "currency_rates.arrow")
    output_filestring_unfiltered = makepath(OUTPUT_DIR, "currency_rates_unfiltered.arrow")

    Arrow.write(output_filestring, currency_table)
    Arrow.write(output_filestring_unfiltered, currency_table_cip_violated)

    time_duration = round(time() - time_start, digits=2)
    println("Finished refining currency data in $time_duration seconds")
end

function termcheck(level::String, term)
    if term == "european"
        return level
    elseif term == "american"
        return INVERTED_RATE_LEVELS[level]
    else
        error("Invalid term type: $term")
    end
end
function termcheck(rate::AbstractVector{T}, term) where T<:Union{Missing,Float64}
    if term == "european"
        return rate
    elseif term == "american"
        return 1 ./ rate
    else
        error("Invalid term type: $term")
    end
end

function build_rate_series(currency, info, rate_data)
    currency_info = info[info[:, :cur_code] .== currency, :]
    dates = first(values(rate_data))[!, :date]

    currency_rate_sets = Dict(
        (a, b) => RateSet() for (a,b) in RATE_TYPES
    )

    for series_source in eachrow(currency_info)
        push_source_to_rate_sets!(currency_rate_sets, series_source, rate_data)
    end

    index_columns = OrderedDict(:cur_code => currency, :date => dates)
    rate_columns = OrderedDict(
        Symbol.("$(a)_$b") => layer_series(currency_rate_sets[(a, b)])
        for (a, b) in RATE_TYPES
    )

    rate_series = DataFrame(merge(index_columns, rate_columns))

    return rate_series
end

function push_source_to_rate_sets!(currency_rate_sets, series_source, rate_data)
    for level in RATE_LEVELS
        spot_rate_code = series_source.symbol_s
        forward_rate_code = series_source.symbol_f
        term = series_source.terms
        checked_level = termcheck(level, term)



        spot_series = rate_data[("spot", checked_level)][!, spot_rate_code]
        forward_series = add_forward_points(
            rate_data[("forward", checked_level)][!, forward_rate_code],
            spot_series,
            series_source.f_denom
        )
        
        push!(currency_rate_sets[("spot", level)], termcheck(spot_series, term))
        push!(
            currency_rate_sets[("forward", level)], termcheck(forward_series, term)
        )
    end
end

function add_forward_points(forward_points, spot_series, f_denom::Integer)
    return forward_points ./ f_denom .+ spot_series
end
add_forward_points(forward_series, ::Any, ::Missing) = forward_series


function layer_series(rate_set)
    # The input data is sorted so that sources in later rows have higher priority
    series_size = length(first(rate_set))
    layered_series = fill(missing, series_size)

    for series in Iterators.reverse(rate_set)
        layered_series = coalesce.(layered_series, series)
    end

    return layered_series
end

function assert_equal_dates!(rate_data)
    # Only dates represented in every set are useful. Asserting equality of dates now
    # prevents the need to merge rate data on dates later.
    date_columns = [rate_data[(a, b)].date for (a, b) in RATE_TYPES]

    latest_start_date = maximum([minimum(dates) for dates in date_columns])
    earliest_end_date = minimum([maximum(dates) for dates in date_columns])

    for (a, b) in RATE_TYPES
        post_start_date(row) = row.date .>= latest_start_date
        pre_end_date(row) = row.date .<= earliest_end_date

        filter!(row -> post_start_date(row) .& pre_end_date(row), rate_data[(a, b)])
    end

    date_collection = [rate_data[(a, b)].date for (a, b) in RATE_TYPES]
    
    dates_conform(date_series) = all(date_series .== first(date_collection))

    if !all(date_series -> dates_conform(date_series), date_collection)
        error("The rate data cannot be conformed to a common date series.")
    end
end

function remove_cip_violations!(currency_table)
    for i in CIP_VIOLATIONS
        cip_violation_mask = (
            (currency_table[:, :cur_code] .== i.currency)
            .& (currency_table[:, :date] .>= Date(i.start_date))
            .& (currency_table[:, :date] .<= Date(i.end_date))
        )
        rate_columns = RATE_TYPE_NAMES

        currency_table[cip_violation_mask, rate_columns] .= missing
    end
end

function end_euro_constituents!(currency_table)
    for i in EURO_CONSTITUENTS
        euro_constituent_mask = (
            (currency_table[:, :cur_code] .== i.currency)
            .& (currency_table[:, :date] .>= Date(i.start_date))
        )

        deleteat!(currency_table, euro_constituent_mask)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end