using DataFrames
using Arrow
using ShiftedArrays: lead, lag
using Dates

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using .CommonFunctions
using .CommonConstants

const INPUT_DIR = joinpath(DIRS.fund, "domicile-grouped")
const OUTPUT_DIR = joinpath(DIRS.fund, "post-processing", "@option", "main")

const ID_COLUMNS = [:name, :fundid, :secid]

const COUNTRY_GROUPS = [
    "lux", "kor", "usa", "can-chn-jpn", "irl-bra",
    "gbr-fra-ind", "esp-tha-aus-zaf-mex-aut-che"
]

const FIELD_FOLDERS = [
    "local-monthly-gross-returns", "local-monthly-net-returns", "monthly-costs",
    "monthly-morningstar-category", "monthly-net-assets", "usd-monthly-gross-returns",
    "usd-monthly-net-returns"
]

function main()
    x = load_dataset("other")
end

function load_dataset(country_group)
    info_filestring = joinpath(INPUT_DIR, "info", "mf_info_$country_group.csv")
    mf_info = CSV.read(info_filestring, DataFrame)

    data_field_set = DataFrame[]
    for folder in FIELD_FOLDERS
        push_data_part!(data_field_set, folder, country_group)
    end

    mf_data = reduce((x,y) -> outerjoin(x,y,on=[ID_COLUMNS..., :date]), data_field_set)

    return mf_data
end

function push_data_part!(data_field_set, folder, country_group)
    filestring = joinpath(INPUT_FILESTRING_BASE, folder, "mf_$(folder)_$country_group.csv")
    data_field = CSV.read(filestring, DataFrame)

    if folder == "info"
        push!(data_field_set, data_field)
    else
        stacked_data = (
            stack(data_field, Not(ID_COLUMNS), variable_name=:date, value_name=folder)
        )
        push!(data_field_set, stacked_data)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end