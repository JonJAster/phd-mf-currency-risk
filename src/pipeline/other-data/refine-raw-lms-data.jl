using Revise
using DataFrames
using CSV
using Arrow

includet("../../shared/CommonConstants.jl")
includet("../../shared/CommonFunctions.jl")

using .CommonConstants
using .CommonFunctions

function refine_raw_lms_data()
    task_start = time()

    usa_filename = joinpath(DIRS.eq.raw, "usa-lms.csv")
    region_filename = joinpath(DIRS.eq.raw, "region-lms.csv")

    usa_data = CSV.read(usa_filename, DataFrame; dateformat="yyyy-mm-dd")
    region_data = CSV.read(region_filename, DataFrame; dateformat="yyyy-mm-dd")

    _init_factor_data!(usa_data)
    _init_factor_data!(region_data)

    lms_factors_full = vcat(usa_data, region_data)

    lms_factors = _filter_to_desired_factors(lms_factors_full, EQUITY_LMS_FACTORS)

    printtime("refining raw equity data", task_start, minutes=false)
    return lms_factors
end

function _init_factor_data!(data)
    rename!(
        data,
        :location => :region,
        :name => :factor
    )

    region_name_map = Dict(
        "developed" => "DEV",
        "emerging" => "EMG",
        "usa" => "USA"
    )

    map_name(name) = get(region_name_map, name, "")

    data.region = map_name.(data.region)

    select!(data, [:region, :date, :factor, :ret])
    return data
end

function _filter_to_desired_factors(lms_factors_full, factor_names_map)
    desired_lms_factors = keys(EQUITY_LMS_FACTORS) |> Set
    lms_factors = (
        lms_factors_full[in.(lms_factors_full.factor, Ref(desired_lms_factors)), :]
    )

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