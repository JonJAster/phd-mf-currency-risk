using Revise
using DataFrames
using CSV
using Arrow
using Dates

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function refine_raw_jkp_lms_data()
    task_start = time()

    usa_filename = joinpath(DIRS.eq.raw, "jkp-usa-lms.csv")
    region_filename = joinpath(DIRS.eq.raw, "jkp-region-lms.csv")

    usa_data = CSV.read(usa_filename, DataFrame; dateformat="yyyy-mm-dd")
    region_data = CSV.read(region_filename, DataFrame; dateformat="yyyy-mm-dd")

    _init_factor_data!(usa_data)
    _init_factor_data!(region_data)

    lms_factors_full = vcat(usa_data, region_data)

    lms_factors = _filter_to_desired_factors(lms_factors_full, EQUITY_LMS_FACTORS)

    countmap(lms_factors.source_id)

    printtime("refining raw JKP equity data", task_start, minutes=false)
    return lms_factors
end

function _init_factor_data!(data)
    rename!(
        data,
        :location => :region,
        :name => :factor
    )

    source_id_map = Dict(
        "developed" => "jkp_dev",
        "emerging" => "jkp_emg",
        "usa" => "jkp_usa",
        "world" => "jkp_wld"
    )

    map_name(name) = get(source_id_map, name, "")

    data.date = firstdayofmonth.(data.date)
    data.source_id = map_name.(data.region)
    
    sort!(data, [:source_id, :factor, :date])
    select!(data, [:source_id, :factor, :date, :ret])
    return data
end

function _filter_to_desired_factors(lms_factors_full, factor_names_map)
    desired_lms_factors = keys(EQUITY_LMS_FACTORS) |> Set
    
    factors_condition(df) = in.(df.factor, Ref(desired_lms_factors))
    source_condition(df) = df.source_id .!= ""
    filter_condition(df) = factors_condition(df) .&& source_condition(df)

    lms_factors = lms_factors_full[filter_condition(lms_factors_full), :]

    lms_factors.factor = map(x -> factor_names_map[x], lms_factors.factor)
    return lms_factors
end

if abspath(PROGRAM_FILE) == @__FILE__
    output_data = refine_raw_lms_data()
    output_filestring = makepath(DIRS.eq.factors, "lms.arrow")
    
    task_start = time()
    Arrow.write(output_filestring, output_data)
    printtime("writing refined equity data", task_start, minutes=false)
end