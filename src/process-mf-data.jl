using DataFrames
using Arrow
using ShiftedArrays: lead, lag
using Dates

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using .CommonFunctions
using .CommonConstants

const INPUT_DIR = joinpath(DIRS.fund, "domicile-grouped")
const OUTPUT_DIR = joinpath(DIRS.fund, "post-processing")

const ID_COLUMNS = [:name, :fundid, :secid]

const COUNTRY_GROUPS = ["usa"]

const FIELD_FOLDERS = [
    "local-monthly-gross-returns", "local-monthly-net-returns", "monthly-costs",
    "monthly-morningstar-category", "monthly-net-assets", "usd-monthly-gross-returns",
    "usd-monthly-net-returns"
]

function main(options=:default)
    time_start = time()

    options == :default && (options = copy(DEFAULT_OPTIONS))
    _fill_empty_options!(options)
    options_folder = option_foldername(; options...)

    for country_group in COUNTRY_GROUPS
        # country_group = "usa"
        println("Loading data for $country_group...")
        df_info = _load_info(country_group)
        # TODO This still takes too long to run because the resultant dataframe is too big
        # for memory.
        df_data = _load_dataset(country_group) 

        println("Processing data for $country_group...")
        !options[:raw_ret_only] && _fill_missing_returns!(df_data)

        _trim_missing_tails!(df_data, id_level=:secid)
        println("")
    end
end

function _load_info(country_group)
    info_filestring = joinpath(INPUT_DIR, "info", "mf_info_$country_group.arrow")
    mf_info = Arrow.Table(info_filestring) |> DataFrame

    return mf_info
end

function _load_dataset(country_group)
    data_field_set = DataFrame[]
    for folder in FIELD_FOLDERS
        filestring = joinpath(INPUT_DIR, folder, "mf_$(folder)_$country_group.arrow")
        data_field = Arrow.Table(filestring) |> DataFrame

        stacked_data = (
            stack(data_field, Not(ID_COLUMNS), variable_name=:date, value_name=folder)
        )
        push!(data_field_set, stacked_data)
    end

    mf_data = reduce((x,y) -> outerjoin(x,y, on=[ID_COLUMNS..., :date]), data_field_set)

    return mf_data
end

function _fill_empty_options!(options)
    if !haskey(options, :currency_type)
        error("Key :currency_type must be specified in options.")
    end

    if !haskey(options, :raw_ret_only)
        options[:raw_ret_only] = true
    end

    if !haskey(options, :polation_method)
        options[:polation_method] = false
    end

    if !haskey(options, :exc_finre)
        options[:exc_finre] = false
    end

    if !haskey(options, :age_filter)
        options[:age_filter] = false
    end

    return
end

function _fill_missing_returns!(df)
    df.ret_gross_m_recalculated = (
        ((df.ret_net_m/100 + 1)/(1-df.rep_costs/100) - 1) * 100
    )

    # There are an inordinate number of ret_net_m observations equal to
    # exactly zero (125x more frequently occuring than the next most
    # common return value to 5 decimal places). Clearly, gross returns
    # in these cases should not be expected to have exactly offset
    # costs, so set gross returns also to zero for those observations.
    df[df_ret_net_m == 0, :ret_gross_m_recalculated] = 0

    df.ret_gross_m = coalesce.(df.ret_gross_m, df.ret_gross_m_recalculated)
end

function _trim_missing_tails!(df; id_level)
    """
    Remove any observations before the first non-missing return across all secids of
    a given fundid, or across an aggregated fundid, as well as any observations after
    the last non-missing return.
    """
    # id_level = secid
    if id_level == :secid
        gb_funddate = groupby(df, [:fundid, :date])
        transform!(gb_funddate, :ret_gross_m => count => :nonmissing_ret_count)
        df.nonmissing_ret_count = (
            groupby(df, [:fundid, :date])
        )

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end