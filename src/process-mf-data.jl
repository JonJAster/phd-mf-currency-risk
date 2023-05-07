using
    DataFrames,
    Arrow,
    Dates

using ShiftedArrays: lead, lag

include("shared/CommonConstants.jl")
include("shared/CommonFunctions.jl")
using
    .CommonFunctions,
    .CommonConstants

const INPUT_DIR = joinpath(DIRS.fund, "domicile-grouped")
const OUTPUT_DIR = joinpath(DIRS.fund, "post-processing")

const ID_COLUMNS = [:name, :fundid, :secid]
const DATE_FORMAT = "yyyy-mm"

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
    time_start = time()
    x = load_dataset("other")
    println(time() - time_start)

   #for group in COUNTRY_GROUPS

end

function load_dataset(country_group)
    # filestring_info = joinpath(INPUT_DIR, "info", "mf_info_$country_group.arrow")
    # mf_info = Arrow.Table(filestring_info) |> DataFrame

    data_field_set = DataFrame[]
    for folder in FIELD_FOLDERS
        push_data_part!(data_field_set, folder, country_group)
    end

    mf_data = reduce((x,y) -> outerjoin(x,y,on=[ID_COLUMNS..., :date]), data_field_set)

    return mf_data
end

function push_data_part!(data_field_set, folder, country_group)
    filestring = joinpath(INPUT_DIR, folder, "mf_$(folder)_$country_group.arrow")
    data_field = Arrow.Table(filestring) |> DataFrame

    if folder == "info"
        push!(data_field_set, data_field)
    else

        stacked_data = (
            stack(data_field, Not(ID_COLUMNS), variable_name=:date, value_name=folder)
        )

        push!(data_field_set, stacked_data)
    end
end

readdate(str) = Date(str, DATE_FORMAT) |> Dates.lastdayofmonth

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end

# AUTO TEST
data_field_set = DataFrame[]
folder = "local-monthly-gross-returns"
country_group = "other"
# MANUAL TEST
if false

end